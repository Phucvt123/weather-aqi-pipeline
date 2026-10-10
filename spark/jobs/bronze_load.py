"""
bronze_load.py — Đọc JSON từ MinIO landing zone, append vào Bronze Iceberg tables.

Usage:
    spark-submit --master spark://spark-master:7077 \
        /opt/spark-apps/spark/jobs/bronze_load.py \
        --execution-time 2025-06-15T14:00:00+00:00 \
        --batch-id 2025-06-15_14_abc12345

Bronze tables (append-only, giữ MỌI phiên bản forecast):
    nessie.bronze.weather_raw
    nessie.bronze.aqi_raw

Idempotent: dùng batch_id để tránh ghi trùng.
"""
import argparse
import sys
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, ArrayType,
)

# Add parent to path for imports
sys.path.insert(0, "/opt/spark-apps")
from spark.common.session import get_spark_session, ensure_namespaces


def create_bronze_tables(spark):
    """Tạo Bronze tables nếu chưa tồn tại."""

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.weather_raw (
            city_code       STRING,
            timestamp_local TIMESTAMP,
            temperature_c   DOUBLE,
            humidity_pct    DOUBLE,
            windspeed_kmh   DOUBLE,
            apparent_temp   DOUBLE,
            weather_code    INT,
            surface_pressure DOUBLE,
            wind_direction  DOUBLE,
            ingested_at     TIMESTAMP,
            batch_id        STRING,
            source          STRING
        )
        USING iceberg
        PARTITIONED BY (days(ingested_at))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.bronze.aqi_raw (
            city_code       STRING,
            timestamp_local TIMESTAMP,
            pm2_5           DOUBLE,
            pm10            DOUBLE,
            aqi_eu          INT,
            us_aqi          INT,
            ingested_at     TIMESTAMP,
            batch_id        STRING,
            source          STRING
        )
        USING iceberg
        PARTITIONED BY (days(ingested_at))
    """)


def _parse_weather_landing(spark, landing_path: str, batch_id: str, ingested_at: str):
    """Đọc weather JSON từ landing, explode arrays thành rows."""

    # Đọc tất cả JSON files trong landing path
    raw_json = spark.read.option("multiline", "true").json(landing_path)

    if raw_json.rdd.isEmpty():
        print(f"  No weather data found at {landing_path}")
        return None

    # Parse và explode hourly arrays
    parsed = (
        raw_json
        .withColumn("city_code", F.col("_city_code"))
        .withColumn("times", F.col("hourly.time"))
        .withColumn("temperature", F.col("hourly.temperature_2m"))
        .withColumn("humidity", F.col("hourly.relative_humidity_2m"))
        .withColumn("windspeed", F.col("hourly.wind_speed_10m"))
        .withColumn("apparent", F.col("hourly.apparent_temperature"))
        .withColumn("wcode", F.col("hourly.weather_code"))
        .withColumn("pressure", F.col("hourly.surface_pressure"))
        .withColumn("wind_dir", F.col("hourly.wind_direction_10m"))
    )

    exploded = (
        parsed
        .withColumn(
            "zipped",
            F.arrays_zip(
                "times", "temperature", "humidity", "windspeed",
                "apparent", "wcode", "pressure", "wind_dir",
            ),
        )
        .withColumn("record", F.explode("zipped"))
        .select(
            "city_code",
            F.to_timestamp(F.col("record.times")).alias("timestamp_local"),
            F.col("record.temperature").cast("double").alias("temperature_c"),
            F.col("record.humidity").cast("double").alias("humidity_pct"),
            F.col("record.windspeed").cast("double").alias("windspeed_kmh"),
            F.col("record.apparent").cast("double").alias("apparent_temp"),
            F.col("record.wcode").cast("int").alias("weather_code"),
            F.col("record.pressure").cast("double").alias("surface_pressure"),
            F.col("record.wind_dir").cast("double").alias("wind_direction"),
            F.to_timestamp(F.lit(ingested_at)).alias("ingested_at"),
            F.lit(batch_id).alias("batch_id"),
            F.lit("open_meteo").alias("source"),
        )
    )
    return exploded


def _parse_aqi_landing(spark, landing_path: str, batch_id: str, ingested_at: str):
    """Đọc AQI JSON từ landing, explode arrays thành rows."""

    raw_json = spark.read.option("multiline", "true").json(landing_path)

    if raw_json.rdd.isEmpty():
        print(f"  No AQI data found at {landing_path}")
        return None

    parsed = (
        raw_json
        .withColumn("city_code", F.col("_city_code"))
        .withColumn("times", F.col("hourly.time"))
        .withColumn("pm2_5_arr", F.col("hourly.pm2_5"))
        .withColumn("pm10_arr", F.col("hourly.pm10"))
        .withColumn("eu_aqi_arr", F.col("hourly.european_aqi"))
        .withColumn("us_aqi_arr", F.col("hourly.us_aqi"))
    )

    exploded = (
        parsed
        .withColumn(
            "zipped",
            F.arrays_zip("times", "pm2_5_arr", "pm10_arr", "eu_aqi_arr", "us_aqi_arr"),
        )
        .withColumn("record", F.explode("zipped"))
        .select(
            "city_code",
            F.to_timestamp(F.col("record.times")).alias("timestamp_local"),
            F.col("record.pm2_5_arr").cast("double").alias("pm2_5"),
            F.col("record.pm10_arr").cast("double").alias("pm10"),
            F.col("record.eu_aqi_arr").cast("int").alias("aqi_eu"),
            F.col("record.us_aqi_arr").cast("int").alias("us_aqi"),
            F.to_timestamp(F.lit(ingested_at)).alias("ingested_at"),
            F.lit(batch_id).alias("batch_id"),
            F.lit("open_meteo").alias("source"),
        )
    )
    return exploded


def main():
    parser = argparse.ArgumentParser(description="Bronze load from landing zone")
    parser.add_argument("--execution-time", required=True, help="ISO timestamp")
    parser.add_argument("--batch-id", required=True, help="Unique batch identifier")
    args = parser.parse_args()

    exec_dt = datetime.fromisoformat(args.execution_time)
    dt_str = exec_dt.strftime("%Y-%m-%d")
    hour_str = exec_dt.strftime("%H")

    print("=" * 60)
    print(f"  BRONZE LOAD — dt={dt_str} hour={hour_str} batch={args.batch_id}")
    print("=" * 60)

    spark = get_spark_session("BronzeLoad")
    ensure_namespaces(spark)
    create_bronze_tables(spark)

    try:
        # Kiểm tra idempotent: nếu batch_id đã tồn tại → skip
        existing = spark.sql(
            f"SELECT COUNT(*) as cnt FROM nessie.bronze.weather_raw "
            f"WHERE batch_id = '{args.batch_id}'"
        ).collect()[0].cnt

        if existing > 0:
            print(f"  ⚠ Batch {args.batch_id} already loaded, skipping (idempotent)")
            return

        # ── Load Weather ──
        weather_path = f"s3a://lake/landing/source=weather/dt={dt_str}/hour={hour_str}/"
        print(f"\n[1/2] Loading weather from {weather_path}")
        weather_df = _parse_weather_landing(spark, weather_path, args.batch_id, args.execution_time)

        if weather_df is not None:
            weather_count = weather_df.count()
            weather_df.writeTo("nessie.bronze.weather_raw").append()
            print(f"  ✓ Appended {weather_count} weather rows to bronze")
        else:
            print("  ⚠ No weather data to load")

        # ── Load AQI ──
        aqi_path = f"s3a://lake/landing/source=aqi/dt={dt_str}/hour={hour_str}/"
        print(f"\n[2/2] Loading AQI from {aqi_path}")
        aqi_df = _parse_aqi_landing(spark, aqi_path, args.batch_id, args.execution_time)

        if aqi_df is not None:
            aqi_count = aqi_df.count()
            aqi_df.writeTo("nessie.bronze.aqi_raw").append()
            print(f"  ✓ Appended {aqi_count} AQI rows to bronze")
        else:
            print("  ⚠ No AQI data to load")

        print("\n  ✅ Bronze load completed successfully")

    except Exception as e:
        print(f"\n  ❌ Bronze load failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

import logging
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, 
    IntegerType, TimestampType
)
from spark.thresholds import THRESHOLDS
from ingestion.config import DB_CONFIG

logger = logging.getLogger(__name__)
# JDBC connection properties
JDBC_URL = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
JDBC_PROPS = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": "org.postgresql.Driver",
}


def get_risk_level(aqi_value):
    if aqi_value is None:
        return None
    for level, range_info in THRESHOLDS.items():
        if range_info["min"] <= aqi_value <= range_info["max"]:
            return level
    return "Hazardous"

def create_spark_session():
    """Tạo SparkSession với PostgreSQL JDBC driver."""
    return (
        SparkSession.builder
        .appName("WeatherAQI_Transform")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        .config("spark.sql.session.timeZone", "Asia/Bangkok")
        .getOrCreate()
    )

def read_raw_weather(spark):
    """
    Đọc raw.raw_weather, parse JSONB payload thành rows.
    1 raw row (chứa 24-48 hourly data points) → explode thành nhiều rows.
    """
    raw_df = (
        spark.read.jdbc(JDBC_URL, "raw.raw_weather", properties=JDBC_PROPS)
        .select("id", "payload", "ingested_at")
    )
    parsed_df = (
        raw_df.withColumn("city_code", F.get_json_object("payload","$._city_code"))
        .withColumn("times", F.from_json(
            F.get_json_object("payload", "$.hourly.time"),
            "ARRAY<STRING>"
        ))
        .withColumn("temperature", F.from_json(
            F.get_json_object("payload", "$.hourly.temperature_2m"),
            "ARRAY<FLOAT>"
        ))
        .withColumn("humidity", F.from_json(
            F.get_json_object("payload", "$.hourly.relative_humidity_2m"),
            "ARRAY<FLOAT>"
        ))
        .withColumn("windspeed", F.from_json(
            F.get_json_object("payload", "$.hourly.wind_speed_10m"),
            "ARRAY<FLOAT>"
        ))
    )
    explode_df = (
        parsed_df
        .withColumn("zipped", F.arrays_zip("times", "temperature", "humidity", "windspeed"))
        .withColumn("record", F.explode("zipped"))
        .select(
            'city_code',
            'ingested_at',
            F.to_timestamp(F.col('record.times')).alias('timestamp_local'),
            F.col('record.temperature').alias('temperature_c'),
            F.col('record.humidity').alias('humidity_pct'),
            F.col('record.windspeed').alias('windspeed_kmh'),
        )
    )
    return explode_df

def read_raw_aqi(spark):
    """
    Đọc raw.raw_aqi, parse JSONB payload thành rows.
    Tương tự weather nhưng lấy PM2.5, PM10, European AQI.
    """
    raw_df = (
        spark.read.jdbc(JDBC_URL, "raw.raw_aqi", properties=JDBC_PROPS)
        .select("id", "payload", "ingested_at")
    )

    parsed_df = (
        raw_df
        .withColumn("city_code", F.get_json_object("payload", "$._city_code"))
        .withColumn("times", F.from_json(
            F.get_json_object("payload", "$.hourly.time"),
            "ARRAY<STRING>"
        ))
        .withColumn("pm2_5", F.from_json(
            F.get_json_object("payload", "$.hourly.pm2_5"),
            "ARRAY<FLOAT>"
        ))
        .withColumn("pm10", F.from_json(
            F.get_json_object("payload", "$.hourly.pm10"),
            "ARRAY<FLOAT>"
        ))
        .withColumn("aqi_eu", F.from_json(
            F.get_json_object("payload", "$.hourly.european_aqi"),
            "ARRAY<INT>"
        ))
    )

    exploded_df = (
        parsed_df
        .withColumn("zipped", F.arrays_zip("times", "pm2_5", "pm10", "aqi_eu"))
        .withColumn("record", F.explode("zipped"))
        .select(
            "city_code",
            "ingested_at",
            F.to_timestamp(F.col("record.times")).alias("timestamp_local"),
            F.col("record.pm2_5").alias("pm2_5"),
            F.col("record.pm10").alias("pm10"),
            F.col("record.aqi_eu").alias("aqi_eu"),
        )
    )

    return exploded_df
def clean_data(df, value_columns):
    """
    Clean: forward-fill null trong window 2 giờ.
    Nếu null quá 3 giờ liên tiếp → drop row đó.
    """
    window_spec = (
        Window
        .partitionBy("city_code")
        .orderBy("timestamp_local")
        .rowsBetween(-2, 0) 
    )

    # Forward-fill: lấy last non-null value trong window 2 giờ
    for col_name in value_columns:
        df = df.withColumn(
            col_name,
            F.coalesce(
                F.col(col_name),
                F.last(F.col(col_name), ignorenulls=True).over(window_spec)
            )
        )

    df = df.dropna(subset=value_columns, how="all")

    return df

def deduplicate(df, key_columns):
    """
    Nếu cùng city + timestamp có nhiều records (do fetch nhiều lần),
    giữ bản mới nhất (ingested_at lớn nhất).
    """
    window_spec = (
        Window
        .partitionBy(*key_columns)
        .orderBy(F.col("ingested_at").desc())
    )
    return (
        df.withColumn(
            "row_num",
            F.row_number().over(window_spec)
        )
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

def join_weather_aqi(weather_df, aqi_df):
    """
    Join weather + AQI theo city_code và timestamp.
    Exact join vì cả 2 API đều trả hourly data cùng timestamp format.
    """
    joined_df = (
        weather_df.join(
            aqi_df,
            on = ['city_code', 'timestamp_local'],
            how = 'outer'
        )
    )
    return joined_df

def enrich_with_risk_level(df):
    """Thêm cột health_risk dựa trên European AQI value."""
    risk_udf = F.udf(get_risk_level, StringType())
    return df.withColumn("health_risk", risk_udf(F.col("aqi_eu")))

def generate_forecast_alerts(df):
    """
    Tìm các rows trong tương lai có health_risk >= Unhealthy.
    Tạo alert với lead_time_hours.
    """
    now = datetime.now()
    alerts_df = (
        df.filter(
            F.col("timestamp_local") > now
        )
        .filter(F.col("health_risk").isin("Unhealthy", "Hazardous"))
        .withColumn("forecast_for_time", F.col("timestamp_local"))
        .withColumn(
            "lead_time_hours",
            ((F.unix_timestamp("timestamp_local") - F.unix_timestamp(F.lit(now))) / 3600).cast("int")
        )
        .withColumn(
            "message",
            F.concat(
                F.lit("Dự báo "),
                F.col("city_code"),
                F.lit(": AQI = "),
                F.col("aqi_eu").cast("string"),
                F.lit(" ("),
                F.col("health_risk"),
                F.lit(") lúc "),
                F.date_format("timestamp_local", "HH:mm dd/MM"),
                F.lit(". PM2.5 = "),
                F.col("pm2_5").cast("string"),
                F.lit(" µg/m³")
            )
        ).select(
            "city_code",
            F.col("city_code").alias("city_name"),
            "timestamp_local",
            "health_risk",
            F.col("aqi_eu").alias("aqi_value"),
            "message",
            "forecast_for_time",
            "lead_time_hours",
        )

    )
    return alerts_df

def write_to_processed(df, table_name, mode="append"):
    """Ghi DataFrame vào processed schema qua JDBC, tránh ghi trùng khóa."""
    if mode == "append":
        spark = df.sparkSession
        if table_name == "hourly_metrics":
            try:
                existing_df = (
                    spark.read
                    .jdbc(JDBC_URL, f"processed.{table_name}", properties=JDBC_PROPS)
                    .select("city_code", "timestamp_local")
                )
                df = df.join(existing_df, on=["city_code", "timestamp_local"], how="left_anti")
                logger.info(f"Filtered out existing records for {table_name}. Remaining rows to insert: {df.count()}")
            except Exception as err:
                logger.warning(f"Could not read existing data from processed.{table_name}: {err}. Writing directly...")
        elif table_name == "forecast_alerts":
            try:
                existing_df = (
                    spark.read
                    .jdbc(JDBC_URL, f"processed.{table_name}", properties=JDBC_PROPS)
                    .select("city_code", "forecast_for_time")
                )
                df = df.join(existing_df, on=["city_code", "forecast_for_time"], how="left_anti")
                logger.info(f"Filtered out existing records for {table_name}. Remaining rows to insert: {df.count()}")
            except Exception as err:
                logger.warning(f"Could not read existing data from processed.{table_name}: {err}. Writing directly...")

    (
        df.write
        .mode(mode)
        .jdbc(JDBC_URL, f"processed.{table_name}", properties=JDBC_PROPS)
    )
    
def run_spark_transform():
    """
    Main function — Airflow gọi hàm này.
    Full flow: read raw → clean → deduplicate → join → enrich → write processed.
    """
    logger.info("Starting Spark transform...")
    spark = create_spark_session()

    try:
        # 1. Read raw
        weather_df = read_raw_weather(spark)
        aqi_df = read_raw_aqi(spark)

        # 2. Deduplicate (giữ bản mới nhất cho mỗi city + timestamp)
        weather_df = deduplicate(weather_df, ["city_code", "timestamp_local"])
        aqi_df = deduplicate(aqi_df, ["city_code", "timestamp_local"])

        # 3. Clean
        weather_df = clean_data(weather_df, ["temperature_c", "humidity_pct", "windspeed_kmh"])
        aqi_df = clean_data(aqi_df, ["pm2_5", "pm10", "aqi_eu"])

        # 4. Join
        joined_df = join_weather_aqi(
            weather_df.drop("ingested_at"),
            aqi_df.drop("ingested_at")
        )

        # 5. Enrich
        enriched_df = enrich_with_risk_level(joined_df)

        # 6. Write hourly_metrics
        metrics_df = enriched_df.select(
            "city_code", "timestamp_local",
            "temperature_c", "humidity_pct", "windspeed_kmh",
            "pm2_5", "pm10", "aqi_eu", "health_risk"
        )
        write_to_processed(metrics_df, "hourly_metrics")

        # 7. Generate & write forecast alerts
        alerts_df = generate_forecast_alerts(enriched_df)
        if alerts_df.count() > 0:
            write_to_processed(alerts_df, "forecast_alerts")
            logger.warning(f"Generated {alerts_df.count()} forecast alerts!")

        logger.info("Spark transform completed successfully")

    except Exception as e:
        logger.error(f"Spark transform failed: {e}")
        raise
    finally:
        spark.stop()
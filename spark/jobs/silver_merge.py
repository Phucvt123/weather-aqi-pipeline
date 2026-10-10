"""
silver_merge.py — Đọc incremental từ Bronze, MERGE INTO Silver.

Sửa lỗi #1 (forecast không update): dùng MERGE INTO thay cho left_anti.
Sửa lỗi #2 (full scan): chỉ đọc Bronze data trong lookback window.

Usage:
    spark-submit --master spark://spark-master:7077 \
        /opt/spark-apps/spark/jobs/silver_merge.py \
        --execution-time 2025-06-15T14:00:00+00:00 \
        --lookback-hours 3

Silver tables:
    nessie.silver.hourly_metrics    — MERGE INTO (upsert)
    nessie.silver.forecast_history  — Append-only (mọi phiên bản forecast)
"""
import argparse
import sys
from datetime import datetime, timedelta

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/spark-apps")
from spark.common.session import get_spark_session, ensure_namespaces
from spark.common.transforms import (
    deduplicate, forward_fill, join_weather_aqi, add_health_risk,
)


def create_silver_tables(spark):
    """Tạo Silver tables nếu chưa tồn tại."""

    # Partition by months(timestamp_local) — lý do:
    #   - ~500K rows/year ÷ 12 months = ~42K rows/partition → vừa đủ
    #   - Nếu partition by day → 365 partitions/year × many small files = over-partitioned
    #   - Sort by (city_code, timestamp_local) để tối ưu range queries
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.hourly_metrics (
            city_code        STRING,
            city_name        STRING,
            timestamp_local  TIMESTAMP,
            temperature_c    DOUBLE,
            humidity_pct     DOUBLE,
            windspeed_kmh    DOUBLE,
            pm2_5            DOUBLE,
            pm10             DOUBLE,
            aqi_eu           INT,
            health_risk      STRING,
            ingested_at      TIMESTAMP,
            updated_at       TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(timestamp_local))
    """)

    # Forecast history — append-only, lưu MỌI phiên bản forecast
    # Đây là bảng cho phép tính forecast accuracy ở Gold layer
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.silver.forecast_history (
            city_code        STRING,
            target_time      TIMESTAMP,
            issued_at        TIMESTAMP,
            temperature_c    DOUBLE,
            humidity_pct     DOUBLE,
            pm2_5            DOUBLE,
            aqi_eu           INT,
            lead_time_hours  INT
        )
        USING iceberg
        PARTITIONED BY (months(target_time))
    """)


# City names dict — loaded from config at the top level
CITY_NAMES = {
    "AG": "An Giang", "BRVT": "Bà Rịa-Vũng Tàu", "BG": "Bắc Giang",
    "BK": "Bắc Kạn", "BL": "Bạc Liêu", "BN": "Bắc Ninh",
    "BTE": "Bến Tre", "BD": "Bình Định", "BDG": "Bình Dương",
    "BP": "Bình Phước", "BTH": "Bình Thuận", "CM": "Cà Mau",
    "CT": "Cần Thơ", "CB": "Cao Bằng", "DN": "Đà Nẵng",
    "DLK": "Đắk Lắk", "DNO": "Đắk Nông", "DB": "Điện Biên",
    "DNG": "Đồng Nai", "DT": "Đồng Tháp", "GL": "Gia Lai",
    "HGI": "Hà Giang", "HNA": "Hà Nam", "HN": "Hà Nội",
    "HT": "Hà Tĩnh", "HD": "Hải Dương", "HP": "Hải Phòng",
    "HG": "Hậu Giang", "HB": "Hoà Bình", "HCM": "Hồ Chí Minh",
    "HY": "Hưng Yên", "KH": "Khánh Hoà", "KG": "Kiên Giang",
    "KT": "Kon Tum", "LC": "Lai Châu", "LD": "Lâm Đồng",
    "LS": "Lạng Sơn", "LCA": "Lào Cai", "LA": "Long An",
    "ND": "Nam Định", "NA": "Nghệ An", "NB": "Ninh Bình",
    "NT": "Ninh Thuận", "PT": "Phú Thọ", "PY": "Phú Yên",
    "QB": "Quảng Bình", "QNA": "Quảng Nam", "QNG": "Quảng Ngãi",
    "QNI": "Quảng Ninh", "QT": "Quảng Trị", "ST": "Sóc Trăng",
    "SL": "Sơn La", "TNI": "Tây Ninh", "TB": "Thái Bình",
    "TN": "Thái Nguyên", "TH": "Thanh Hoá", "HUE": "Thừa Thiên Huế",
    "TGI": "Tiền Giang", "TV": "Trà Vinh", "TQ": "Tuyên Quang",
    "VL": "Vĩnh Long", "VP": "Vĩnh Phúc", "YB": "Yên Bái",
}


def main():
    parser = argparse.ArgumentParser(description="Silver MERGE INTO from Bronze")
    parser.add_argument("--execution-time", required=True)
    parser.add_argument("--lookback-hours", type=int, default=3)
    args = parser.parse_args()

    exec_dt = datetime.fromisoformat(args.execution_time)
    lookback_start = (exec_dt - timedelta(hours=args.lookback_hours)).isoformat()

    print("=" * 60)
    print(f"  SILVER MERGE — execution={args.execution_time}")
    print(f"  Lookback: {lookback_start} → {args.execution_time}")
    print("=" * 60)

    spark = get_spark_session("SilverMerge")
    ensure_namespaces(spark)
    create_silver_tables(spark)

    try:
        # ── Step 1: Incremental read from Bronze ──
        print("\n[1/5] Reading incremental weather from Bronze...")
        weather_df = spark.sql(f"""
            SELECT city_code, timestamp_local, temperature_c, humidity_pct,
                   windspeed_kmh, ingested_at
            FROM nessie.bronze.weather_raw
            WHERE ingested_at >= TIMESTAMP '{lookback_start}'
        """)
        w_count = weather_df.count()
        print(f"  Found {w_count} weather rows")

        print("\n[2/5] Reading incremental AQI from Bronze...")
        aqi_df = spark.sql(f"""
            SELECT city_code, timestamp_local, pm2_5, pm10, aqi_eu,
                   ingested_at
            FROM nessie.bronze.aqi_raw
            WHERE ingested_at >= TIMESTAMP '{lookback_start}'
        """)
        a_count = aqi_df.count()
        print(f"  Found {a_count} AQI rows")

        if w_count == 0 and a_count == 0:
            print("\n  ⚠ No new data in Bronze, skipping Silver merge")
            return

        # ── Step 2: Dedup + Clean ──
        print("\n[3/5] Dedup + forward-fill...")
        weather_clean = forward_fill(
            deduplicate(weather_df, ["city_code", "timestamp_local"]),
            ["temperature_c", "humidity_pct", "windspeed_kmh"],
        )
        aqi_clean = forward_fill(
            deduplicate(aqi_df, ["city_code", "timestamp_local"]),
            ["pm2_5", "pm10", "aqi_eu"],
        )

        # ── Step 3: Join + Enrich ──
        joined = join_weather_aqi(
            weather_clean.drop("ingested_at"),
            aqi_clean,
        )
        enriched = add_health_risk(joined)

        # Add city_name
        city_map = F.create_map(
            *[item for code, name in CITY_NAMES.items()
              for item in (F.lit(code), F.lit(name))]
        )
        enriched = enriched.withColumn(
            "city_name",
            F.coalesce(city_map[F.col("city_code")], F.col("city_code")),
        ).withColumn("updated_at", F.current_timestamp())

        # ── Step 4: MERGE INTO silver.hourly_metrics ──
        # Đây là fix cho bug #1: forecast giờ được UPDATE khi có dữ liệu mới hơn
        print("\n[4/5] MERGE INTO silver.hourly_metrics...")
        enriched.createOrReplaceTempView("new_data")

        merge_result = spark.sql("""
            MERGE INTO nessie.silver.hourly_metrics t
            USING new_data s
            ON t.city_code = s.city_code AND t.timestamp_local = s.timestamp_local
            WHEN MATCHED AND s.ingested_at > t.ingested_at THEN
                UPDATE SET
                    t.city_name = s.city_name,
                    t.temperature_c = s.temperature_c,
                    t.humidity_pct = s.humidity_pct,
                    t.windspeed_kmh = s.windspeed_kmh,
                    t.pm2_5 = s.pm2_5,
                    t.pm10 = s.pm10,
                    t.aqi_eu = s.aqi_eu,
                    t.health_risk = s.health_risk,
                    t.ingested_at = s.ingested_at,
                    t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN
                INSERT (city_code, city_name, timestamp_local,
                        temperature_c, humidity_pct, windspeed_kmh,
                        pm2_5, pm10, aqi_eu, health_risk,
                        ingested_at, updated_at)
                VALUES (s.city_code, s.city_name, s.timestamp_local,
                        s.temperature_c, s.humidity_pct, s.windspeed_kmh,
                        s.pm2_5, s.pm10, s.aqi_eu, s.health_risk,
                        s.ingested_at, s.updated_at)
        """)
        print("  ✓ MERGE INTO completed")

        # ── Step 5: Append to forecast_history ──
        print("\n[5/5] Appending to silver.forecast_history...")
        exec_ts = F.to_timestamp(F.lit(args.execution_time))

        forecast_df = (
            enriched
            .filter(F.col("timestamp_local") > exec_ts)
            .withColumn("target_time", F.col("timestamp_local"))
            .withColumn("issued_at", F.col("ingested_at"))
            .withColumn(
                "lead_time_hours",
                (
                    (F.unix_timestamp("timestamp_local") - F.unix_timestamp(exec_ts))
                    / 3600
                ).cast("int"),
            )
            .select(
                "city_code", "target_time", "issued_at",
                "temperature_c", "humidity_pct", "pm2_5", "aqi_eu",
                "lead_time_hours",
            )
        )

        fc_count = forecast_df.count()
        if fc_count > 0:
            forecast_df.writeTo("nessie.silver.forecast_history").append()
            print(f"  ✓ Appended {fc_count} forecast history rows")
        else:
            print("  ⚠ No forecast rows to append")

        print("\n  ✅ Silver merge completed successfully")

    except Exception as e:
        print(f"\n  ❌ Silver merge failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

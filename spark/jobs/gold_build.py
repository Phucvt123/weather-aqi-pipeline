"""
gold_build.py — Xây dựng Gold tables từ Silver.

Gold tables:
    nessie.gold.forecast_alerts      — Cảnh báo AQI tương lai
    nessie.gold.city_daily_summary   — Tổng hợp ngày theo thành phố
    nessie.gold.forecast_accuracy    — MAE/bias theo lead time (điểm nhấn project)

Usage:
    spark-submit --master spark://spark-master:7077 \
        /opt/spark-apps/spark/jobs/gold_build.py \
        --execution-time 2025-06-15T14:00:00+00:00
"""
import argparse
import sys

from pyspark.sql import functions as F

sys.path.insert(0, "/opt/spark-apps")
from spark.common.session import get_spark_session, ensure_namespaces
from spark.common.transforms import generate_alerts

# City names (duplicated here for standalone execution)
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


def create_gold_tables(spark):
    """Tạo Gold tables nếu chưa tồn tại."""

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.forecast_alerts (
            city_code        STRING,
            city_name        STRING,
            timestamp_local  TIMESTAMP,
            health_risk      STRING,
            aqi_value        INT,
            message          STRING,
            forecast_for_time TIMESTAMP,
            lead_time_hours  INT,
            created_at       TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(created_at))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.city_daily_summary (
            city_code        STRING,
            city_name        STRING,
            date_local       DATE,
            temp_min         DOUBLE,
            temp_max         DOUBLE,
            temp_avg         DOUBLE,
            humidity_avg     DOUBLE,
            aqi_min          INT,
            aqi_max          INT,
            aqi_avg          DOUBLE,
            hours_unhealthy  INT,
            hours_hazardous  INT
        )
        USING iceberg
        PARTITIONED BY (months(date_local))
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.gold.forecast_accuracy (
            city_code         STRING,
            lead_time_bucket  STRING,
            mae               DOUBLE,
            bias              DOUBLE,
            rmse              DOUBLE,
            sample_count      BIGINT,
            computed_at       TIMESTAMP
        )
        USING iceberg
    """)


def build_forecast_alerts(spark, execution_time: str):
    """Tạo alerts từ Silver hourly_metrics."""
    print("\n[1/3] Building forecast_alerts...")

    metrics_df = spark.sql("SELECT * FROM nessie.silver.hourly_metrics")
    alerts_df = generate_alerts(metrics_df, execution_time, CITY_NAMES)

    alert_count = alerts_df.count()
    if alert_count > 0:
        # Ghi đè hoàn toàn — alerts luôn được tính lại mỗi giờ
        alerts_df.writeTo("nessie.gold.forecast_alerts").overwritePartitions()
        print(f"  ✓ Generated {alert_count} alerts")
    else:
        print("  ✓ No alerts (air quality is good!)")


def build_city_daily_summary(spark):
    """Tổng hợp daily summary từ Silver."""
    print("\n[2/3] Building city_daily_summary...")

    city_map = F.create_map(
        *[item for code, name in CITY_NAMES.items()
          for item in (F.lit(code), F.lit(name))]
    )

    summary_df = spark.sql("""
        SELECT
            city_code,
            CAST(timestamp_local AS DATE) as date_local,
            MIN(temperature_c) as temp_min,
            MAX(temperature_c) as temp_max,
            AVG(temperature_c) as temp_avg,
            AVG(humidity_pct) as humidity_avg,
            MIN(aqi_eu) as aqi_min,
            MAX(aqi_eu) as aqi_max,
            AVG(CAST(aqi_eu AS DOUBLE)) as aqi_avg,
            SUM(CASE WHEN health_risk = 'Unhealthy' THEN 1 ELSE 0 END) as hours_unhealthy,
            SUM(CASE WHEN health_risk = 'Hazardous' THEN 1 ELSE 0 END) as hours_hazardous
        FROM nessie.silver.hourly_metrics
        WHERE timestamp_local <= current_timestamp()
        GROUP BY city_code, CAST(timestamp_local AS DATE)
    """)

    summary_df = summary_df.withColumn(
        "city_name",
        F.coalesce(city_map[F.col("city_code")], F.col("city_code")),
    )

    count = summary_df.count()
    summary_df.writeTo("nessie.gold.city_daily_summary").overwritePartitions()
    print(f"  ✓ Generated {count} daily summaries")


def build_forecast_accuracy(spark):
    """
    Tính forecast accuracy — ĐIỂM NHẤN CỦA PROJECT.

    So sánh giá trị forecast (từ forecast_history) với giá trị thực tế
    (từ hourly_metrics) theo lead time buckets: 1h, 6h, 12h, 24h.

    Chỉ tính được nhờ Bronze giữ lại MỌI phiên bản forecast.
    """
    print("\n[3/3] Building forecast_accuracy...")

    # Join forecast với actual
    accuracy_df = spark.sql("""
        WITH forecast AS (
            SELECT
                city_code,
                target_time,
                aqi_eu as forecast_aqi,
                temperature_c as forecast_temp,
                lead_time_hours
            FROM nessie.silver.forecast_history
            WHERE lead_time_hours > 0
        ),
        actual AS (
            SELECT
                city_code,
                timestamp_local,
                aqi_eu as actual_aqi,
                temperature_c as actual_temp
            FROM nessie.silver.hourly_metrics
            WHERE timestamp_local <= current_timestamp()
        ),
        joined AS (
            SELECT
                f.city_code,
                f.lead_time_hours,
                f.forecast_aqi,
                a.actual_aqi,
                f.forecast_temp,
                a.actual_temp,
                CASE
                    WHEN f.lead_time_hours <= 1  THEN '1h'
                    WHEN f.lead_time_hours <= 6  THEN '6h'
                    WHEN f.lead_time_hours <= 12 THEN '12h'
                    WHEN f.lead_time_hours <= 24 THEN '24h'
                    ELSE '24h+'
                END as lead_time_bucket
            FROM forecast f
            INNER JOIN actual a
                ON f.city_code = a.city_code
                AND f.target_time = a.timestamp_local
            WHERE f.forecast_aqi IS NOT NULL
              AND a.actual_aqi IS NOT NULL
        )
        SELECT
            city_code,
            lead_time_bucket,
            AVG(ABS(forecast_aqi - actual_aqi)) as mae,
            AVG(CAST(forecast_aqi - actual_aqi AS DOUBLE)) as bias,
            SQRT(AVG(POWER(CAST(forecast_aqi - actual_aqi AS DOUBLE), 2))) as rmse,
            COUNT(*) as sample_count
        FROM joined
        GROUP BY city_code, lead_time_bucket
    """).withColumn("computed_at", F.current_timestamp())

    count = accuracy_df.count()
    if count > 0:
        # Overwrite — luôn tính lại từ đầu
        accuracy_df.writeTo("nessie.gold.forecast_accuracy").overwritePartitions()
        print(f"  ✓ Computed accuracy for {count} city×bucket combinations")

        # Show sample
        print("\n  Sample accuracy results:")
        accuracy_df.orderBy("lead_time_bucket", "city_code").show(20, truncate=False)
    else:
        print("  ⚠ Not enough data yet for accuracy calculation")


def main():
    parser = argparse.ArgumentParser(description="Gold layer build")
    parser.add_argument("--execution-time", required=True)
    args = parser.parse_args()

    print("=" * 60)
    print(f"  GOLD BUILD — execution={args.execution_time}")
    print("=" * 60)

    spark = get_spark_session("GoldBuild")
    ensure_namespaces(spark)
    create_gold_tables(spark)

    try:
        build_forecast_alerts(spark, args.execution_time)
        build_city_daily_summary(spark)
        build_forecast_accuracy(spark)
        print("\n  ✅ Gold build completed successfully")
    except Exception as e:
        print(f"\n  ❌ Gold build failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

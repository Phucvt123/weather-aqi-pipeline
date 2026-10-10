"""
maintenance.py — Iceberg table maintenance operations.

Operations:
    1. rewrite_data_files (compaction)  — gộp small files
    2. expire_snapshots                 — xoá snapshots cũ hơn N ngày
    3. remove_orphan_files              — dọn file rác trên MinIO

Usage:
    spark-submit --master spark://spark-master:7077 \
        /opt/spark-apps/spark/jobs/maintenance.py \
        --retention-days 7
"""
import argparse
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/opt/spark-apps")
from spark.common.session import get_spark_session


ALL_TABLES = [
    "nessie.bronze.weather_raw",
    "nessie.bronze.aqi_raw",
    "nessie.silver.hourly_metrics",
    "nessie.silver.forecast_history",
    "nessie.gold.forecast_alerts",
    "nessie.gold.city_daily_summary",
    "nessie.gold.forecast_accuracy",
]


def compact_table(spark, table: str):
    """Compaction: gộp small files thành files lớn hơn (~128MB target)."""
    print(f"\n  Compacting {table}...")
    try:
        result = spark.sql(f"""
            CALL nessie.system.rewrite_data_files(
                table => '{table}',
                options => map('target-file-size-bytes', '134217728')
            )
        """)
        row = result.collect()[0]
        print(
            f"    Rewritten: {row['rewritten_data_files_count']} files "
            f"→ {row['added_data_files_count']} files"
        )
    except Exception as e:
        print(f"    ⚠ Compact failed (table may not exist or be empty): {e}")


def expire_snapshots(spark, table: str, retention_days: int):
    """Xoá snapshots cũ để giải phóng storage."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    print(f"\n  Expiring snapshots older than {cutoff} for {table}...")
    try:
        result = spark.sql(f"""
            CALL nessie.system.expire_snapshots(
                table => '{table}',
                older_than => TIMESTAMP '{cutoff}',
                retain_last => 5
            )
        """)
        row = result.collect()[0]
        print(f"    Expired: {row['deleted_data_files_count']} data files, "
              f"{row['deleted_manifest_files_count']} manifests")
    except Exception as e:
        print(f"    ⚠ Expire failed: {e}")


def remove_orphans(spark, table: str):
    """Xoá orphan files — files trên storage nhưng không thuộc snapshot nào."""
    cutoff = (datetime.now(timezone.utc) - timedelta(days=3)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    print(f"\n  Removing orphan files for {table}...")
    try:
        result = spark.sql(f"""
            CALL nessie.system.remove_orphan_files(
                table => '{table}',
                older_than => TIMESTAMP '{cutoff}'
            )
        """)
        rows = result.collect()
        print(f"    Removed: {len(rows)} orphan files")
    except Exception as e:
        print(f"    ⚠ Remove orphans failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Iceberg table maintenance")
    parser.add_argument("--retention-days", type=int, default=7)
    parser.add_argument("--compact-only", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print(f"  LAKE MAINTENANCE — retention={args.retention_days} days")
    print("=" * 60)

    spark = get_spark_session("LakeMaintenance")

    try:
        for table in ALL_TABLES:
            print(f"\n{'─' * 40}")
            print(f"  Table: {table}")
            print(f"{'─' * 40}")

            compact_table(spark, table)

            if not args.compact_only:
                expire_snapshots(spark, table, args.retention_days)
                remove_orphans(spark, table)

        print("\n  ✅ Maintenance completed successfully")

    except Exception as e:
        print(f"\n  ❌ Maintenance failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

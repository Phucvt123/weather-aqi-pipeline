"""
session.py — SparkSession factory cho Iceberg + Nessie + MinIO.

Khi chạy qua spark-submit, tất cả config đã có trong spark-defaults.conf.
File này chỉ tạo session và đảm bảo các namespace tồn tại.
"""
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "WeatherAQI_Lakehouse") -> SparkSession:
    """
    Tạo SparkSession. Config Iceberg/Nessie/MinIO lấy từ spark-defaults.conf.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    return spark


def ensure_namespaces(spark: SparkSession):
    """Tạo các namespace (database) nếu chưa tồn tại."""
    for ns in ["bronze", "silver", "gold"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.{ns}")

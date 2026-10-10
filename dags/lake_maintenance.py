"""
lake_maintenance.py — DAG bảo trì lakehouse.

Chạy hằng ngày lúc 03:00 UTC:
    1. Compaction (rewrite_data_files)
    2. Expire snapshots (giữ 7 ngày)
    3. Remove orphan files
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lake_maintenance",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 3 * * *",  # Daily at 03:00 UTC
    catchup=False,
    tags=["lakehouse", "maintenance"],
    doc_md="""
    ## Lake Maintenance
    
    Chạy hằng ngày lúc 03:00 UTC:
    - **Compaction**: Gộp small files → ít files lớn hơn → query nhanh hơn
    - **Expire snapshots**: Xoá snapshots cũ hơn 7 ngày → tiết kiệm storage
    - **Remove orphans**: Dọn file rác trên MinIO
    """,
) as dag:

    maintenance = SparkSubmitOperator(
        task_id="run_maintenance",
        application="/opt/spark-apps/spark/jobs/maintenance.py",
        conn_id="spark_default",
        application_args=["--retention-days", "7"],
        verbose=True,
    )

    maintenance

"""
hourly_ingest.py — DAG chính chạy mỗi giờ.

Luồng:
    1. ingest_to_landing   → Gọi API, ghi JSON lên MinIO
    2. bronze_load         → Spark: landing → Bronze (append)
    3. silver_merge        → Spark: Bronze → Silver (MERGE INTO)
    4. gold_build          → Spark: Silver → Gold (alerts, summary, accuracy)
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def _ingest_to_landing(**context):
    """Python task: gọi API và ghi JSON lên MinIO landing zone."""
    from ingestion.landing_writer import ingest_to_landing

    # Dùng data_interval_end thay cho datetime.now() (fix bug #3)
    execution_time = context["data_interval_end"].isoformat()
    result = ingest_to_landing(execution_time=execution_time)
    # Push batch_id để downstream tasks dùng
    context["ti"].xcom_push(key="batch_id", value=result["batch_id"])
    return result


with DAG(
    dag_id="hourly_ingest",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["lakehouse", "weather", "aqi"],
    doc_md="""
    ## Hourly Ingestion Pipeline (Lakehouse)
    
    Chạy mỗi giờ: lấy dữ liệu từ Open-Meteo API cho 63 tỉnh thành,
    ghi vào MinIO landing zone, rồi transform qua Bronze → Silver → Gold.
    """,
) as dag:

    # Task 1: Ingest to landing (Python — chạy trên Airflow worker)
    ingest_task = PythonOperator(
        task_id="ingest_to_landing",
        python_callable=_ingest_to_landing,
    )

    # Task 2: Landing → Bronze (Spark — chạy trên Spark cluster)
    bronze_task = SparkSubmitOperator(
        task_id="bronze_load",
        application="/opt/spark-apps/spark/jobs/bronze_load.py",
        conn_id="spark_default",
        application_args=[
            "--execution-time", "{{ data_interval_end.isoformat() }}",
            "--batch-id", "{{ ti.xcom_pull(task_ids='ingest_to_landing', key='batch_id') }}",
        ],
        verbose=True,
    )

    # Task 3: Bronze → Silver (Spark)
    silver_task = SparkSubmitOperator(
        task_id="silver_merge",
        application="/opt/spark-apps/spark/jobs/silver_merge.py",
        conn_id="spark_default",
        application_args=[
            "--execution-time", "{{ data_interval_end.isoformat() }}",
            "--lookback-hours", "3",
        ],
        verbose=True,
    )

    # Task 4: Silver → Gold (Spark)
    gold_task = SparkSubmitOperator(
        task_id="gold_build",
        application="/opt/spark-apps/spark/jobs/gold_build.py",
        conn_id="spark_default",
        application_args=[
            "--execution-time", "{{ data_interval_end.isoformat() }}",
        ],
        verbose=True,
    )

    ingest_task >> bronze_task >> silver_task >> gold_task

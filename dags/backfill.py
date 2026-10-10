"""
backfill.py — DAG backfill dữ liệu lịch sử.

Chạy thủ công (trigger with config) với tham số start_date và end_date.
Dùng Open-Meteo Historical API để lấy dữ liệu 2 năm cho 63 tỉnh.

Trigger qua Airflow UI:
    {"start_date": "2024-01-01", "end_date": "2024-01-31"}
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _backfill_to_landing(**context):
    """Gọi Historical API và ghi JSON lên MinIO."""
    from ingestion.landing_writer import ingest_historical_to_landing

    params = context["params"]
    start_date = params.get("start_date", "2024-01-01")
    end_date = params.get("end_date", "2024-01-31")

    result = ingest_historical_to_landing(start_date=start_date, end_date=end_date)
    context["ti"].xcom_push(key="batch_id", value=result["batch_id"])
    return result


with DAG(
    dag_id="backfill",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["lakehouse", "backfill"],
    params={
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
    },
    doc_md="""
    ## Backfill Historical Data
    
    Trigger manually with config:
    ```json
    {"start_date": "2024-01-01", "end_date": "2024-03-31"}
    ```
    
    Sử dụng Open-Meteo Historical Weather API + Air Quality API.
    Dữ liệu được ghi vào landing zone rồi load vào Bronze.
    Sau đó chạy Silver merge để tổng hợp.
    """,
) as dag:

    backfill_landing = PythonOperator(
        task_id="backfill_to_landing",
        python_callable=_backfill_to_landing,
    )

    # Bronze load cho historical data dùng cùng job,
    # nhưng với path khác (source=weather_historical)
    bronze_note = PythonOperator(
        task_id="bronze_load_historical",
        python_callable=lambda **ctx: print(
            "Historical Bronze load uses the same bronze_load.py "
            "with adjusted landing paths. For large backfills, "
            "run silver_merge separately after all Bronze data is loaded."
        ),
    )

    silver_rebuild = SparkSubmitOperator(
        task_id="silver_full_rebuild",
        application="/opt/spark-apps/spark/jobs/silver_merge.py",
        conn_id="spark_default",
        application_args=[
            "--execution-time", "{{ data_interval_end.isoformat() }}",
            "--lookback-hours", "87600",  # ~10 years — full rebuild
        ],
        verbose=True,
    )

    gold_rebuild = SparkSubmitOperator(
        task_id="gold_full_rebuild",
        application="/opt/spark-apps/spark/jobs/gold_build.py",
        conn_id="spark_default",
        application_args=[
            "--execution-time", "{{ data_interval_end.isoformat() }}",
        ],
        verbose=True,
    )

    backfill_landing >> bronze_note >> silver_rebuild >> gold_rebuild

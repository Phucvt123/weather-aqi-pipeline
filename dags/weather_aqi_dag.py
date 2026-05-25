from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ingestion.fetch_weather import fetch_weather_data
from ingestion.fetch_aqi import fetch_aqi_data
from spark.transform import run_spark_transform

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_aqi_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",  
    catchup=False,
    tags=["weather", "aqi"],
) as dag:

    fetch_weather_task = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather_data,
    )

    fetch_aqi_task = PythonOperator(
        task_id="fetch_aqi",
        python_callable=fetch_aqi_data,
    )

    transform_task = PythonOperator(
        task_id="run_spark_transform",
        python_callable=run_spark_transform,
    )

    [fetch_weather_task, fetch_aqi_task] >> transform_task

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.ingest_to_bronze import run_all_loads

with DAG(
    dag_id="load_bronze_tables",
    start_date=datetime(2025, 11, 23),
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "dwh"],
) as dag:

    load_bronze = PythonOperator(
        task_id="load_bronze_raw_data",
        python_callable=run_all_loads
    )

    load_bronze

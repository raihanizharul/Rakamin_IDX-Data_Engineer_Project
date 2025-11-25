from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.bronze_to_silver import run_all_loads

with DAG(
    dag_id="load_silver_tables",
    start_date=datetime(2025, 11, 23),
    schedule_interval="@daily",
    catchup=False,
    tags=["silver", "dwh"],
) as dag:

    load_silver = PythonOperator(
        task_id="load_silver_clean_data",
        python_callable=run_all_loads
    )

    load_silver

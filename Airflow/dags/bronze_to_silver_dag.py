from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.bronze_to_silver import run_all_loads
from scripts.silver_quality_check import run_data_quality

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
    ),
    
    quality_check = PythonOperator(
        task_id="silver_data_quality_check",
        python_callable=run_data_quality
    )

    load_silver >> quality_check

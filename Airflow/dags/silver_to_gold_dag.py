from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.silver_to_gold import run_all_loads

with DAG(
    dag_id="load_gold_tables",
    start_date=datetime(2025, 11, 23),
    schedule_interval="@daily",
    catchup=False,
    tags=["gold", "dwh"],
) as dag:

    load_silver = PythonOperator(
        task_id="load_gold_data",
        python_callable=run_all_loads
    )

    load_silver

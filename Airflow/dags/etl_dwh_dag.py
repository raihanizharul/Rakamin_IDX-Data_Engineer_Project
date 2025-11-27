from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.ingest_to_bronze import run_all_loads_bronze
from scripts.bronze_to_silver import run_all_loads_silver
from scripts.silver_quality_check import run_data_quality
from scripts.silver_to_gold import run_all_loads_gold

with DAG(
    dag_id="etl_dwh_pipeline",
    start_date=datetime(2025, 11, 23),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "dwh"],
) as dag:

    load_bronze = PythonOperator(
        task_id="load_bronze_raw_data",
        python_callable=run_all_loads_bronze
    )
    
    load_silver = PythonOperator(
        task_id="load_silver_clean_data",
        python_callable=run_all_loads_silver
    ),
    
    quality_check = PythonOperator(
        task_id="silver_data_quality_check",
        python_callable=run_data_quality
    )
    
    load_gold = PythonOperator(
        task_id="load_gold_data",
        python_callable=run_all_loads_gold
    )

    load_bronze >> load_silver >> quality_check >> load_gold

import pandas as pd
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# ============================================================
# 1. Connect to MS SQL DWH via Airflow Hook
# ============================================================
def get_engine_from_airflow():
    hook = MsSqlHook(mssql_conn_id="mssql_DWH")
    return hook.get_sqlalchemy_engine()


# ============================================================
# 2. Insert DataFrame to Bronze Schema
# ============================================================
def load_to_bronze(df: pd.DataFrame, table_name: str):
    engine = get_engine_from_airflow()
    df.to_sql(
        name=table_name,
        schema="bronze",
        con=engine,
        if_exists="append",
        index=False
    )
    print(f"[OK] Loaded {len(df)} rows into bronze.{table_name}")


# ============================================================
# 3. Load from Excel
# ============================================================
def load_excel_transaction(path_excel: str):
    df = pd.read_excel(path_excel)
    load_to_bronze(df, "transaction_excel_raw")


# ============================================================
# 4. Load dari CSV
# ============================================================
def load_csv_transaction(path_csv: str):
    df = pd.read_csv(path_csv)
    load_to_bronze(df, "transaction_csv_raw")


# ============================================================
# 5. Load dari SQL Express ke Bronze
# ============================================================
def load_from_sql_source(source_table: str, bronze_table: str):
    engine = get_engine_from_airflow()
    query = f"SELECT * FROM {source_table}"
    df = pd.read_sql(query, engine)
    load_to_bronze(df, bronze_table)


# ============================================================
# 6. Fungsi utama yg dipanggil Airflow DAG
# ============================================================
def run_all_loads():
    print("=== START LOADING INTO BRONZE ===")

    load_excel_transaction("/opt/airflow/dags/datasets/transaction_excel.xlsx")
    load_csv_transaction("/opt/airflow/dags/datasets/transaction_excel.csv")

    load_from_sql_source("bank.transaction", "transaction_db_raw")
    load_from_sql_source("bank.account", "account_db_raw")
    load_from_sql_source("bank.customer", "customer_db_raw")
    load_from_sql_source("bank.branch", "branch_db_raw")
    load_from_sql_source("bank.city", "city_db_raw")
    load_from_sql_source("bank.state", "state_db_raw")

    print("=== LOADING COMPLETE ===")

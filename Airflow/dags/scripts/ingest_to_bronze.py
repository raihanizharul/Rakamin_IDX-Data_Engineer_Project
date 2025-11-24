import os
import pandas as pd
import pymssql

# ============================================================
# GLOBAL PATH CONFIG
# ============================================================
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
FOLDER_DATASETS = os.path.join(AIRFLOW_HOME, "dags", "datasets")

# ============================================================
# DATABASE CONNECTION CONFIG
# ============================================================
DB_CONFIG = {
    "server": "host.docker.internal\\SQLEXPRESS01",  # bisa juga 'localhost\\SQLEXPRESS'
    "user": "airflow",
    "password": "admin",
    "database": "DWH",
    "port": 1433  # default SQL Server port
}

# ============================================================
# 1. Connect to SQL Server using pymssql
# ============================================================
def get_connection():
    conn = pymssql.connect(**DB_CONFIG)
    return conn

# ============================================================
# 2. Truncate table
# ============================================================
def truncate_table(table_name: str):
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            sql = f"TRUNCATE TABLE {table_name};"
            cursor.execute(sql)
        conn.commit()
        print(f"[OK] Truncated {table_name}")
    finally:
        conn.close()

# ============================================================
# 3. Insert DataFrame to Bronze Schema
# ============================================================
def load_to_bronze(df: pd.DataFrame, table_name: str):
    conn = get_connection()
    try:
        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        
        with conn.cursor() as cursor:
            cursor.executemany(sql, tuples)
        conn.commit()
        print(f"[OK] Loaded {len(df)} rows into {table_name}")
    finally:
        conn.close()

# ============================================================
# 4. Load from Excel
# ============================================================
def load_excel_transaction(path_excel: str):
    df = pd.read_excel(path_excel, dtype=str)
    load_to_bronze(df, "bronze.transaction_excel_raw")

# ============================================================
# 5. Load from CSV
# ============================================================
def load_csv_transaction(path_csv: str):
    df = pd.read_csv(path_csv, dtype=str)
    load_to_bronze(df, "bronze.transaction_csv_raw")

# ============================================================
# 6. Load from SQL Source (SQL Server)
# ============================================================
def load_from_sql_source(source_table: str, bronze_table: str):
    conn = get_connection()
    try:
        query = f"SELECT * FROM {source_table}"
        df = pd.read_sql(query, conn)
        load_to_bronze(df, bronze_table)
    finally:
        conn.close()

# ============================================================
# 7. The main function
# ============================================================
def run_all_loads():
    print("=== START LOADING INTO BRONZE ===")

    # --- FULL LOAD ---
    for table in [
        "bronze.transaction_excel_raw",
        "bronze.transaction_csv_raw",
        "bronze.transaction_db_raw",
        "bronze.account_db_raw",
        "bronze.customer_db_raw",
        "bronze.branch_db_raw",
        "bronze.city_db_raw",
        "bronze.state_db_raw"
    ]:
        truncate_table(table)

    # --- Load from Files ---
    csv_path = os.path.join(FOLDER_DATASETS, "transaction_csv.csv")
    excel_path = os.path.join(FOLDER_DATASETS, "transaction_excel.xlsx")
    load_csv_transaction(csv_path)
    load_excel_transaction(excel_path)

    # --- Load from SQL Source ---
    load_from_sql_source("sample.dbo.transaction_db", "bronze.transaction_db_raw")
    load_from_sql_source("sample.dbo.account", "bronze.account_db_raw")
    load_from_sql_source("sample.dbo.customer", "bronze.customer_db_raw")
    load_from_sql_source("sample.dbo.branch", "bronze.branch_db_raw")
    load_from_sql_source("sample.dbo.city", "bronze.city_db_raw")
    load_from_sql_source("sample.dbo.state", "bronze.state_db_raw")

    print("=== LOADING COMPLETE ===")

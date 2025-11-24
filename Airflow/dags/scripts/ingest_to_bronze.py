import os
import pandas as pd
import pymssql
import time
from datetime import datetime

# ============================================================
# GLOBAL PATH CONFIG
# ============================================================
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
FOLDER_DATASETS = os.path.join(AIRFLOW_HOME, "dags", "datasets")

# ============================================================
# DATABASE CONNECTION CONFIG
# ============================================================
DB_CONFIG = {
    "server": "host.docker.internal\\SQLEXPRESS01",
    "user": "airflow",
    "password": "admin",
    "database": "DWH",
    "port": 1433
}

# ============================================================
# Utility for timing
# ============================================================
def log_time(message: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")

def measure_time(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        label = ""
        
        # Custom labeling rules
        if func.__name__ == "load_from_sql_source":
            source = args[0]
            dwh = args[1]
            label = f" (source={source} → dwh={dwh})"

        elif func.__name__ == "load_csv_transaction":
            file = os.path.basename(args[0])
            # bronze table sudah fixed
            dwh = "bronze.transaction_csv_raw"
            label = f" (source={file} → dwh={dwh})"

        elif func.__name__ == "load_excel_transaction":
            file = os.path.basename(args[0])
            dwh = "bronze.transaction_excel_raw"
            label = f" (source={file} → dwh={dwh})"

        elif func.__name__ == "truncate_table":
            table = args[0]
            label = f" (table={table})"

        # run_all_loads → tidak pakai label

        log_time(f"START: {func.__name__}{label}")

        try:
            return func(*args, **kwargs)
        except Exception as e:
            log_time(f"ERROR in {func.__name__}: {str(e)}")
            raise
        finally:
            duration = round(time.time() - start, 2)
            log_time(f"END: {func.__name__}{label} | Duration: {duration} seconds")

    return wrapper


# ============================================================
# 1. Connect to SQL Server using pymssql
# ============================================================
def get_connection():
    return pymssql.connect(**DB_CONFIG)

# ============================================================
# 2. Truncate table
# ============================================================
@measure_time
def truncate_table(table_name: str):
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name};")
        conn.commit()
        print(f"[OK] Truncated {table_name}")
    except Exception as e:
        raise RuntimeError(f"Gagal truncate {table_name}: {str(e)}")
    finally:
        conn.close()

# ============================================================
# 3. Insert DataFrame to Bronze Schema
# ============================================================
def load_to_bronze(df: pd.DataFrame, table_name: str):
    try:
        conn = get_connection()

        tuples = [tuple(x) for x in df.to_numpy()]
        cols = ",".join(df.columns)
        placeholders = ",".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

        with conn.cursor() as cursor:
            cursor.executemany(sql, tuples)

        conn.commit()
        print(f"[OK] Loaded {len(df)} rows into {table_name}")

    except Exception as e:
        raise RuntimeError(f"Gagal insert ke {table_name}: {str(e)}")

    finally:
        conn.close()

# ============================================================
# 4. Load from Excel
# ============================================================
@measure_time
def load_excel_transaction(path_excel: str):
    try:
        df = pd.read_excel(path_excel, dtype=str)
        load_to_bronze(df, "bronze.transaction_excel_raw")
    except Exception as e:
        raise RuntimeError(f"Error load Excel: {str(e)}")

# ============================================================
# 5. Load from CSV
# ============================================================
@measure_time
def load_csv_transaction(path_csv: str):
    try:
        df = pd.read_csv(path_csv, dtype=str)
        load_to_bronze(df, "bronze.transaction_csv_raw")
    except Exception as e:
        raise RuntimeError(f"Error load CSV: {str(e)}")

# ============================================================
# 6. Load from SQL Source (SQL Server)
# ============================================================
@measure_time
def load_from_sql_source(source_table: str, bronze_table: str):
    try:
        conn = get_connection()
        query = f"SELECT * FROM {source_table}"
        df = pd.read_sql(query, conn)
        load_to_bronze(df, bronze_table)
    except Exception as e:
        raise RuntimeError(f"Error load SQL Source {source_table}: {str(e)}")
    finally:
        conn.close()

# ============================================================
# 7. The main function
# ============================================================
@measure_time
def run_all_loads(**context):
    log_time("=== START LOADING INTO BRONZE ===")

    try:
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

    except Exception as e:
        log_time(f"FATAL ERROR in run_all_loads: {str(e)}")
        raise e

    log_time("=== LOADING COMPLETE ===")

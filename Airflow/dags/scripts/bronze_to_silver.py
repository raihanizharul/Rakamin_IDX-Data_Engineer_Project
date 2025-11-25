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

COLUMN_MAPPINGS = {

    # =============================================================================
    # TRANSACTION (Bronze Excel/CSV/DB → Silver.TransactionClean)
    # =============================================================================
    "silver.TransactionClean": {
        "transaction_id": "TransactionId",
        "account_id": "AccountId",
        "transaction_date": "TransactionDate",
        "amount": "Amount",
        "transaction_type": "TransactionType",
        "branch_id": "BranchId"
    },

    # =============================================================================
    # ACCOUNT (bronze.account_db_raw → silver.AccountClean)
    # =============================================================================
    "silver.AccountClean": {
        "account_id": "AccountId",
        "customer_id": "CustomerId",
        "account_type": "AccountType",
        "balance": "Balance",
        "date_opened": "DateOpened",
        "status": "Status"
    },

    # =============================================================================
    # CUSTOMER (bronze.customer_db_raw → silver.CustomerClean)
    # =============================================================================
    "silver.CustomerClean": {
        "customer_id": "CustomerId",
        "customer_name": "CustomerName",
        "address": "Address",
        "city_id": "CityId",
        "age": "Age",
        "gender": "Gender",
        "email": "Email"
    },

    # =============================================================================
    # BRANCH (bronze.branch_db_raw → silver.BranchClean)
    # =============================================================================
    "silver.BranchClean": {
        "branch_id": "BranchId",
        "branch_name": "BranchName",
        "branch_location": "BranchLocation"
    },

    # =============================================================================
    # CITY (bronze.city_db_raw → silver.CityClean)
    # =============================================================================
    "silver.CityClean": {
        "city_id": "CityId",
        "city_name": "CityName",
        "state_id": "StateId"
    },

    # =============================================================================
    # STATE (bronze.state_db_raw → silver.StateClean)
    # =============================================================================
    "silver.StateClean": {
        "state_id": "StateId",
        "state_name": "StateName"
    }
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
            dwh = "bronze.transaction_csv_raw"
            label = f" (source={file} → dwh={dwh})"

        elif func.__name__ == "load_excel_transaction":
            file = os.path.basename(args[0])
            dwh = "bronze.transaction_excel_raw"
            label = f" (source={file} → dwh={dwh})"

        elif func.__name__ == "truncate_table":
            table = args[0]
            label = f" (table={table})"


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
    conn = None
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
    conn = None
    try:
        # --- APPLY MAPPING ---
        if table_name in COLUMN_MAPPINGS:
            mapping = COLUMN_MAPPINGS[table_name]

            # Hanya rename kolom yang ada di mapping
            df = df.rename(columns=mapping)

            # Select hanya kolom yang ada di DB (mapping values)
            df = df[list(mapping.values())]

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
# 4. Load from SQL Source (SQL Server)
# ============================================================
@measure_time
def load_clean_transaction():
    conn = None
    try:
        conn = get_connection()

        # --- Load 3 sumber Bronze ---
        df_excel = pd.read_sql("SELECT * FROM bronze.transaction_excel_raw", conn)
        df_csv = pd.read_sql("SELECT * FROM bronze.transaction_csv_raw", conn)
        df_db = pd.read_sql("SELECT * FROM bronze.transaction_db_raw", conn)

        # --- Satukan semua data ---
        df_all = pd.concat([df_excel, df_csv, df_db], ignore_index=True)

        # --- Hilangkan duplikat berdasarkan semua kolom ---
        df_clean = df_all.drop_duplicates()

        # --- Sort by transaction_id ---
        df_clean = df_clean.sort_values(by='transaction_id', ascending=True)
        
        # --- Insert ke SILVER ---
        load_to_bronze(df_clean, "silver.TransactionClean")

        print(f"[OK] Loaded cleaned transaction: {len(df_clean)} rows")

    except Exception as e:
        raise RuntimeError(f"Error in load_clean_transaction: {str(e)}")

    finally:
        conn.close()


# ============================================================
# 5. Load from SQL Source (SQL Server)
# ============================================================
@measure_time
def load_from_sql_source(source_table: str, bronze_table: str):
    conn = None
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
# 6. The main function
# ============================================================
@measure_time
def run_all_loads(**context):
    log_time("=== START LOADING INTO BRONZE ===")

    try:
        # --- FULL LOAD ---
        for table in [
            "silver.TransactionClean",
            "silver.AccountClean",
            "silver.CustomerClean",
            "silver.BranchClean",
            "silver.CityClean",
            "silver.StateClean"
        ]:
            truncate_table(table)

        '''
            "bronze.transaction_excel_raw",
            "bronze.transaction_csv_raw",
            "bronze.transaction_db_raw",
            "bronze.account_db_raw",
            "bronze.customer_db_raw",
            "bronze.branch_db_raw",
            "bronze.city_db_raw",
            "bronze.state_db_raw"
            
            "silver.TransactionClean",
            "silver.AccountClean",
            "silver.CustomerClean",
            "silver.BranchClean",
            "silver.CityClean",
            "silver.StateClean"
        '''
        # --- Load from SQL Source ---
        load_clean_transaction()
        load_from_sql_source("bronze.account_db_raw", "silver.AccountClean")
        load_from_sql_source("bronze.customer_db_raw", "silver.CustomerClean")
        load_from_sql_source("bronze.branch_db_raw", "silver.BranchClean")
        load_from_sql_source("bronze.city_db_raw", "silver.CityClean")
        load_from_sql_source("bronze.state_db_raw", "silver.StateClean")

    except Exception as e:
        log_time(f"FATAL ERROR in run_all_loads: {str(e)}")
        raise e

    log_time("=== LOADING COMPLETE ===")

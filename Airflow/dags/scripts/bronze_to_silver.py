import os
import pandas as pd
import time
from datetime import datetime
from scripts.conn.db_connection import get_connection

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
# Load Transaction Data
# ============================================================
@measure_time
def load_clean_transaction():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # --- Load 3 transaction data sources ---
        df_excel = pd.read_sql("SELECT * FROM bronze.transaction_excel_raw", conn)
        df_csv = pd.read_sql("SELECT * FROM bronze.transaction_csv_raw", conn)
        df_db = pd.read_sql("SELECT * FROM bronze.transaction_db_raw", conn)

        # --- Merge all data ---
        df_all = pd.concat([df_excel, df_csv, df_db], ignore_index=True)

        # --- Handling Duplicates ---
        df_clean = df_all.drop_duplicates()

        # --- Sort by transaction_id ---
        df_clean = df_clean.sort_values(by='transaction_id', ascending=True)
        
        cursor.execute("TRUNCATE TABLE silver.TransactionClean")
        
        insert_sql = """
            INSERT INTO silver.TransactionClean
                (TransactionID, AccountID, TransactionDate, Amount, TransactionType, BranchID)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        # --- Mapping dataframe → DB ---
        data_to_insert = df_clean[[
            "transaction_id",
            "account_id",
            'transaction_date',
            "amount",
            "transaction_type",
            "branch_id"
        ]].values.tolist()

        # --- Insert ke Silver ---
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()

        print(f"[OK] Loaded cleaned transaction: {len(df_clean)} rows")

    except Exception as e:
        raise RuntimeError(f"Error in load_clean_transaction: {str(e)}")

    finally:
        conn.close()


# ============================================================
# Load Account Data
# ============================================================
@measure_time
def load_clean_account():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # --- Load account source data ---
        df = pd.read_sql("SELECT * FROM bronze.account_db_raw", conn)

        # --- Handling Duplicates ---
        df_clean = df.drop_duplicates()

        # --- Sort by account_id ---
        df_clean = df_clean.sort_values(by='account_id', ascending=True)

        # --- (Opsional) Truncate table silver ---
        cursor.execute("TRUNCATE TABLE silver.AccountClean")

        # --- SQL Insert ---
        insert_sql = """
            INSERT INTO silver.AccountClean
                (AccountID, CustomerID, AccountType, Balance, DateOpened, Status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        # --- Mapping dataframe → DB ---
        data_to_insert = df_clean[[
            "account_id",
            "customer_id",
            "account_type",
            "balance",
            "date_opened",
            "status"
        ]].values.tolist()

        # --- Insert to Silver ---
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()

        print(f"[OK] Loaded cleaned account: {len(df_clean)} rows")

    except Exception as e:
        raise RuntimeError(f"Error in load_clean_account: {str(e)}")

    finally:
        conn.close()


# ============================================================
# Load Branch Data
# ============================================================
@measure_time
def load_clean_branch():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # --- Load account source data ---
        df = pd.read_sql("SELECT * FROM bronze.branch_db_raw", conn)

        # --- Handling Duplicates ---
        df_clean = df.drop_duplicates()

        # --- Sort by account_id ---
        df_clean = df_clean.sort_values(by='branch_id', ascending=True)

        # --- (Opsional) Truncate table silver ---
        cursor.execute("TRUNCATE TABLE silver.BranchClean")

        # --- SQL Insert ---
        insert_sql = """
            INSERT INTO silver.BranchClean
                (BranchID, BranchName, BranchLocation)
            VALUES (%s, %s, %s)
        """

        # --- Mapping dataframe → DB ---
        data_to_insert = df_clean[[
            "branch_id",
            "branch_name",
            "branch_location"
        ]].values.tolist()

        # --- Insert to Silver ---
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()

        print(f"[OK] Loaded cleaned branch: {len(df_clean)} rows")

    except Exception as e:
        raise RuntimeError(f"Error in load_clean_branch: {str(e)}")

    finally:
        conn.close()
        
# ============================================================
# Load City Data
# ============================================================
@measure_time
def load_clean_city():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # --- Load account source data ---
        df = pd.read_sql("SELECT * FROM bronze.city_db_raw", conn)

        # --- Handling Duplicates ---
        df_clean = df.drop_duplicates()
        
        # --- Data Cleaning: Uppercase for string columns ---
        df_clean["city_name"] = df_clean["city_name"].str.upper()

        # --- Sort by account_id ---
        df_clean = df_clean.sort_values(by='city_id', ascending=True)

        # --- (Opsional) Truncate table silver ---
        cursor.execute("TRUNCATE TABLE silver.CityClean")

        # --- SQL Insert ---
        insert_sql = """
            INSERT INTO silver.CityClean
                (CityID, CityName, StateID)
            VALUES (%s, %s, %s)
        """

        # --- Mapping dataframe → DB ---
        data_to_insert = df_clean[[
            "city_id",
            "city_name",
            "state_id"
        ]].values.tolist()

        # --- Insert to Silver ---
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()

        print(f"[OK] Loaded cleaned city: {len(df_clean)} rows")

    except Exception as e:
        raise RuntimeError(f"Error in load_clean_city: {str(e)}")

    finally:
        conn.close()
        

# ============================================================
# Load Customer Data
# ============================================================
@measure_time
def load_clean_customer():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # --- Load account source data ---
        df = pd.read_sql("SELECT * FROM bronze.customer_db_raw", conn)

        # --- Handling Duplicates ---
        df_clean = df.drop_duplicates()

        # --- Data Cleaning: Uppercase for string columns ---
        df_clean["customer_name"] = df_clean["customer_name"].str.upper()
        df_clean["address"] = df_clean["address"].str.upper()
        df_clean["gender"] = df_clean["gender"].str.upper()
        
        # --- Sort by account_id ---
        df_clean = df_clean.sort_values(by='customer_id', ascending=True)

        # --- (Opsional) Truncate table silver ---
        cursor.execute("TRUNCATE TABLE silver.CustomerClean")

        # --- SQL Insert ---
        insert_sql = """
            INSERT INTO silver.CustomerClean
                (CustomerID, CustomerName, Address, CityID, Age, Gender, Email)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        # --- Mapping dataframe → DB ---
        data_to_insert = df_clean[[
            "customer_id",
            "customer_name",
            "address",
            "city_id",
            "age",
            "gender",
            "email"
        ]].values.tolist()

        # --- Insert to Silver ---
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()

        print(f"[OK] Loaded cleaned customer: {len(df_clean)} rows")

    except Exception as e:
        raise RuntimeError(f"Error in load_clean_customer: {str(e)}")

    finally:
        conn.close()


# ============================================================
# Load State Data
# ============================================================
@measure_time
def load_clean_state():
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # --- Load account source data ---
        df = pd.read_sql("SELECT * FROM bronze.state_db_raw", conn)

        # --- Handling Duplicates ---
        df_clean = df.drop_duplicates()
        
        # --- Data Cleaning: Uppercase for string columns ---
        df_clean["state_name"] = df_clean["state_name"].str.upper()

        # --- Sort by account_id ---
        df_clean = df_clean.sort_values(by='state_id', ascending=True)

        # --- (Opsional) Truncate table silver ---
        cursor.execute("TRUNCATE TABLE silver.StateClean")

        # --- SQL Insert ---
        insert_sql = """
            INSERT INTO silver.StateClean
                (StateID, StateName)
            VALUES (%s, %s)
        """

        # --- Mapping dataframe → DB ---
        data_to_insert = df_clean[[
            "state_id",
            "state_name"
        ]].values.tolist()

        # --- Insert to Silver ---
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()

        print(f"[OK] Loaded cleaned state: {len(df_clean)} rows")

    except Exception as e:
        raise RuntimeError(f"Error in load_clean_stater: {str(e)}")

    finally:
        conn.close()
        

# ============================================================
# The main function
# ============================================================
@measure_time
def run_all_loads_silver(**context):
    log_time("=== START LOADING INTO BRONZE ===")

    try:
        # --- Full Load from bronze to silver ---
        load_clean_transaction()
        load_clean_account()
        load_clean_branch()
        load_clean_city()
        load_clean_customer()
        load_clean_state()
 
    except Exception as e:
        log_time(f"FATAL ERROR in run_all_loads: {str(e)}")
        raise e

    log_time("=== LOADING COMPLETE ===")

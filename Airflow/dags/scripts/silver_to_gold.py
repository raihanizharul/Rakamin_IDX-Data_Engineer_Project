import os
import pandas as pd
import pymssql
import time
from datetime import datetime

# ============================================================
# DATABASE CONFIG
# ============================================================
DB_CONFIG = {
    "server": "host.docker.internal\\SQLEXPRESS01",
    "user": "airflow",
    "password": "admin",
    "database": "DWH",
    "port": 1433
}

# ============================================================
# UTILITY FUNCTIONS
# ============================================================
def log_time(message: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")

def measure_time(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        label = f" [{func.__name__}]"

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
# DB CONNECTION
# ============================================================
def get_connection():
    return pymssql.connect(**DB_CONFIG)

# ============================================================
# 1. LOAD DIM BRANCH
# ============================================================
@measure_time
def load_dim_branch():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        df = pd.read_sql("SELECT * FROM silver.BranchClean", conn)
        df_clean = df.drop_duplicates().sort_values("BranchID")

        # Disable FK in Fact (because fact → branch)
        cursor.execute("""
            ALTER TABLE gold.FactTransaction 
            NOCHECK CONSTRAINT FK_FactTransaction_DimBranch;
        """)

        cursor.execute("DELETE FROM gold.DimBranch")

        insert_sql = """
            INSERT INTO gold.DimBranch (BranchID, BranchName, BranchLocation)
            VALUES (%s, %s, %s)
        """
        data = df_clean[["BranchID", "BranchName", "BranchLocation"]].values.tolist()
        cursor.executemany(insert_sql, data)

        # Re-enable FK
        cursor.execute("""
            ALTER TABLE gold.FactTransaction 
            WITH CHECK CHECK CONSTRAINT FK_FactTransaction_DimBranch;
        """)

        conn.commit()
        print(f"[OK] DimBranch loaded: {len(df_clean)} rows")

    finally:
        conn.close()

# ============================================================
# 2. LOAD DIM CUSTOMER
# ============================================================
@measure_time
def load_dim_customer():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        df_cust = pd.read_sql("SELECT * FROM silver.CustomerClean", conn)
        df_city = pd.read_sql("SELECT * FROM silver.CityClean", conn)
        df_state = pd.read_sql("SELECT * FROM silver.StateClean", conn)

        df_join = (
            df_cust
            .merge(df_city, on="CityID", how="left")
            .merge(df_state, on="StateID", how="left")
        ).drop_duplicates().sort_values("CustomerID")

        # Disable FK in DimAccount → DimCustomer
        cursor.execute("""
            ALTER TABLE gold.DimAccount
            NOCHECK CONSTRAINT FK_DimAccount_DimCustomer;
        """)

        # Safe to delete now
        cursor.execute("DELETE FROM gold.DimCustomer")

        insert_sql = """
            INSERT INTO gold.DimCustomer 
                (CustomerID, CustomerName, Address, CityName, StateName, Age, Gender, Email)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """

        data = df_join[
            ["CustomerID", "CustomerName", "Address",
             "CityName", "StateName", "Age", "Gender", "Email"]
        ].values.tolist()

        cursor.executemany(insert_sql, data)

        # Re-enable FK
        cursor.execute("""
            ALTER TABLE gold.DimAccount
            WITH CHECK CHECK CONSTRAINT FK_DimAccount_DimCustomer;
        """)

        conn.commit()
        print(f"[OK] DimCustomer loaded: {len(df_join)} rows")

    finally:
        conn.close()


# ============================================================
# 3. LOAD DIM ACCOUNT
# ============================================================
@measure_time
def load_dim_account():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        df = pd.read_sql("SELECT * FROM silver.AccountClean", conn)
        df_clean = df.drop_duplicates().sort_values("AccountID")

        # Disable BOTH FK:
        # 1) FactTransaction → DimAccount
        # 2) DimAccount → DimCustomer
        cursor.execute("""
            ALTER TABLE gold.FactTransaction 
            NOCHECK CONSTRAINT FK_FactTransaction_DimAccount;
        """)
        cursor.execute("""
            ALTER TABLE gold.DimAccount
            NOCHECK CONSTRAINT FK_DimAccount_DimCustomer;
        """)

        cursor.execute("DELETE FROM gold.DimAccount")

        insert_sql = """
            INSERT INTO gold.DimAccount 
                (AccountID, CustomerID, AccountType, Balance, DateOpened, Status)
            VALUES (%s,%s,%s,%s,%s,%s)
        """

        data = df_clean[
            ["AccountID", "CustomerID", "AccountType",
             "Balance", "DateOpened", "Status"]
        ].values.tolist()

        cursor.executemany(insert_sql, data)

        # Re-Enable FK
        cursor.execute("""
            ALTER TABLE gold.DimAccount
            WITH CHECK CHECK CONSTRAINT FK_DimAccount_DimCustomer;
        """)
        cursor.execute("""
            ALTER TABLE gold.FactTransaction
            WITH CHECK CHECK CONSTRAINT FK_FactTransaction_DimAccount;
        """)

        conn.commit()
        print(f"[OK] DimAccount loaded: {len(df_clean)} rows")

    finally:
        conn.close()

# ============================================================
# 4. LOAD FACT TRANSACTION
# ============================================================
@measure_time
def load_fact_transaction():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        df_trx = pd.read_sql("SELECT * FROM silver.TransactionClean", conn)
        df_acc = pd.read_sql("SELECT AccountID FROM gold.DimAccount", conn)
        df_branch = pd.read_sql("SELECT BranchID FROM gold.DimBranch", conn)

        df_join = (
            df_trx
            .merge(df_acc, on="AccountID", how="left")
            .merge(df_branch, on="BranchID", how="left")
        ).drop_duplicates().sort_values("TransactionID")

        cursor.execute("DELETE FROM gold.FactTransaction")

        insert_sql = """
            INSERT INTO gold.FactTransaction
                (TransactionID, AccountID, BranchID, TransactionDate, Amount, TransactionType)
            VALUES (%s,%s,%s,%s,%s,%s)
        """

        data = df_join[
            ["TransactionID", "AccountID", "BranchID",
             "TransactionDate", "Amount", "TransactionType"]
        ].values.tolist()

        cursor.executemany(insert_sql, data)
        conn.commit()

        print(f"[OK] FactTransaction loaded: {len(df_join)} rows")

    finally:
        conn.close()

# ============================================================
# MAIN RUN ALL
# ============================================================
@measure_time
def run_all_loads_gold(**context):
    log_time("=== START GOLD LOAD ===")

    try:
        load_dim_branch()
        load_dim_customer()
        load_dim_account()
        load_fact_transaction()

    except Exception as e:
        log_time(f"FATAL ERROR: {str(e)}")
        raise

    log_time("=== GOLD LOAD COMPLETE ===")

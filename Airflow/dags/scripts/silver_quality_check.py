import pandas as pd
import pymssql
from datetime import datetime

DB_CONFIG = {
    "server": "host.docker.internal\\SQLEXPRESS01",
    "user": "airflow",
    "password": "admin",
    "database": "DWH",
    "port": 1433
}

def get_connection():
    return pymssql.connect(**DB_CONFIG)

def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ================================================================
# HELPER: Check Uppercase
# ================================================================
def check_uppercase(df, col):
    """
    Return list value yang tidak uppercase.
    """
    if col not in df.columns:
        return []  # kolom tidak ada, skip

    invalid = df[df[col].notnull() & (df[col] != df[col].str.upper())][col].tolist()
    return invalid


# ================================================================
# GENERIC DATA QUALITY CHECK
# ================================================================
def dq_check(table, pk_col, required_cols=[], uppercase_cols=[]):
    """
    Data Quality untuk 1 tabel:
      - Row count
      - Null value check
      - Duplicate PK
      - Uppercase validation
    """
    conn = get_connection()
    log(f"--- CHECKING TABLE: {table} ---")

    df = pd.read_sql(f"SELECT * FROM {table}", conn)

    results = {}

    # 1️⃣ Row count
    results["row_count"] = len(df)

    # 2️⃣ Null Check
    nulls = df[required_cols].isnull().sum()
    results["nulls"] = nulls[nulls > 0].to_dict()

    # 3️⃣ Duplicate PK
    if pk_col in df.columns:
        dups = df[df.duplicated(pk_col)][pk_col].tolist()
    else:
        dups = []
    results["duplicate_pk"] = dups

    # 4️⃣ Uppercase Checks
    uppercase_issues = {}
    for col in uppercase_cols:
        invalid_vals = check_uppercase(df, col)
        if invalid_vals:
            uppercase_issues[col] = invalid_vals
    results["uppercase_issues"] = uppercase_issues

    # Log summary
    log(f"✓ Rows             : {results['row_count']}")
    log(f"✓ Null Values      : {results['nulls']}")
    log(f"✓ Duplicate PK     : {results['duplicate_pk']}")
    log(f"✓ Uppercase Issues : {results['uppercase_issues']}")

    return results


# ================================================================
# RUN ALL TABLE CHECKS
# ================================================================
def run_data_quality():
    log("=== START DATA QUALITY CHECK (NO FK) ===")

    dq_results = {}

    dq_results["CustomerClean"] = dq_check(
        table="silver.CustomerClean",
        pk_col="CustomerID",
        required_cols=["CustomerID", "CustomerName", "Address", "CityID", "Age", "Gender", "Email"],
        uppercase_cols=["CustomerName", "Address", "Gender"]
    )

    dq_results["CityClean"] = dq_check(
        table="silver.CityClean",
        pk_col="CityID",
        required_cols=["CityID", "CityName", "StateID"],
        uppercase_cols=["CityName"]
    )

    dq_results["StateClean"] = dq_check(
        table="silver.StateClean",
        pk_col="StateID",
        required_cols=["StateID", "StateName"],
        uppercase_cols=["StateName"]
    )

    dq_results["TransactionClean"] = dq_check(
        table="silver.TransactionClean",
        pk_col="TransactionID",
        required_cols=["TransactionID", "AccountID", "TransactionDate", "Amount", "TransactionType", "BranchID"],
        uppercase_cols=[]
    )

    dq_results["AccountClean"] = dq_check(
        table="silver.AccountClean",
        pk_col="AccountID",
        required_cols=["AccountID", "CustomerID", "AccountType", "Balance", "DateOpened", "Status"],
        uppercase_cols=[]
    )

    dq_results["BranchClean"] = dq_check(
        table="silver.BranchClean",
        pk_col="BranchID",
        required_cols=["BranchID", "BranchName", "BranchLocation"],
        uppercase_cols=[]
    )

    log("=== DATA QUALITY CHECK COMPLETE ===")

    return dq_results


# ================================================================
# RUN MANUAL
# ================================================================
if __name__ == "__main__":
    results = run_data_quality()
    print("\n=== FINAL DQ REPORT ===")
    print(results)

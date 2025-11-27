import pandas as pd
from datetime import datetime
from scripts.conn.db_connection import get_connection


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# ================================================================
# HELPER: Uppercase check
# ================================================================
def check_uppercase(df, col):
    if col not in df.columns:
        return []
    invalid = df[df[col].notnull() & (df[col] != df[col].str.upper())][col].tolist()
    return invalid


# ================================================================
# GENERIC QUALITY CHECK (+ FK CHECK)
# ================================================================
def dq_check(table, pk_col, required_cols=[], uppercase_cols=[], fk_checks=[]):
    """
    fk_checks = [
        { "col": "CustomerID", "ref_table": "gold.DimCustomer", "ref_col": "CustomerID" }
    ]
    """
    conn = get_connection()
    log(f"--- CHECKING GOLD TABLE: {table} ---")

    df = pd.read_sql(f"SELECT * FROM {table}", conn)
    results = {}

    # 1. Row count
    results["row_count"] = len(df)

    # 2. Null check
    nulls = df[required_cols].isnull().sum()
    results["nulls"] = nulls[nulls > 0].to_dict()

    # 3. Duplicate PK
    if pk_col in df.columns:
        dups = df[df.duplicated(pk_col)][pk_col].tolist()
    else:
        dups = []
    results["duplicate_pk"] = dups

    # 4. Uppercase
    uppercase_issues = {}
    for col in uppercase_cols:
        invalid = check_uppercase(df, col)
        if invalid:
            uppercase_issues[col] = invalid
    results["uppercase_issues"] = uppercase_issues

    # 5. FOREIGN KEY VALIDATION
    fk_result = {}
    for fk in fk_checks:
        col = fk["col"]
        ref_table = fk["ref_table"]
        ref_col = fk["ref_col"]

        log(f"Checking FK: {table}.{col} → {ref_table}.{ref_col}")

        ref_df = pd.read_sql(f"SELECT {ref_col} FROM {ref_table}", conn)

        # nilai FK yang tidak ada di tabel referensi
        invalid_fk = df[~df[col].isin(ref_df[ref_col])][col].unique().tolist()

        if invalid_fk:
            fk_result[f"{col} -> {ref_table}"] = invalid_fk

    results["fk_issues"] = fk_result

    # Log summary
    log(f"✓ Rows             : {results['row_count']}")
    log(f"✓ Null Values      : {results['nulls']}")
    log(f"✓ Duplicate PK     : {results['duplicate_pk']}")
    if uppercase_cols:
        log(f"✓ Uppercase Issues : {results['uppercase_issues']}")
    if fk_checks:
        log(f"✓ FK Issues        : {results['fk_issues']}")

    return results


# ================================================================
# RUN ALL TABLE CHECKS
# ================================================================
def run_data_quality_gold():
    log("=== START DATA QUALITY CHECK: GOLD ===")

    dq_results = {}

    # ----------------------------------------------------
    # DimBranch
    # ----------------------------------------------------
    dq_results["DimBranch"] = dq_check(
        table="gold.DimBranch",
        pk_col="BranchID",
        required_cols=["BranchID", "BranchName", "BranchLocation"],
        uppercase_cols=[],
        fk_checks=[]
    )

    # ----------------------------------------------------
    # DimCustomer
    # ----------------------------------------------------
    dq_results["DimCustomer"] = dq_check(
        table="gold.DimCustomer",
        pk_col="CustomerID",
        required_cols=["CustomerID", "CustomerName", "Address", "CityName", "StateName", "Age", "Gender", "Email"],
        uppercase_cols=["CustomerName", "Address", "CityName", "StateName", "Gender"],
        fk_checks=[]
    )

    # ----------------------------------------------------
    # DimAccount
    # ----------------------------------------------------
    dq_results["DimAccount"] = dq_check(
        table="gold.DimAccount",
        pk_col="AccountID",
        required_cols=["AccountID", "CustomerID", "AccountType", "Balance", "DateOpened", "Status"],
        uppercase_cols=[],
        fk_checks=[
            {"col": "CustomerID", "ref_table": "gold.DimCustomer", "ref_col": "CustomerID"}
        ]
    )

    # ----------------------------------------------------
    # FactTransaction
    # ----------------------------------------------------
    dq_results["FactTransaction"] = dq_check(
        table="gold.FactTransaction",
        pk_col="TransactionID",
        required_cols=["TransactionID", "AccountID", "BranchID", "TransactionDate", "Amount", "TransactionType"],
        uppercase_cols=[],
        fk_checks=[
            {"col": "AccountID", "ref_table": "gold.DimAccount", "ref_col": "AccountID"},
            {"col": "BranchID",  "ref_table": "gold.DimBranch",  "ref_col": "BranchID"},
        ]
    )

    log("=== GOLD DATA QUALITY CHECK COMPLETE ===")
    return dq_results


# ================================================================
# RUN QUALITY CHECK  
# ================================================================
if __name__ == "__main__":
    results = run_data_quality_gold()
    print("\n=== FINAL GOLD DQ REPORT ===")
    print(results)

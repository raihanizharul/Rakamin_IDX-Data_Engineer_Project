import pymssql

DB_CONFIG = {
    "server": "host.docker.internal\\SQLEXPRESS01",
    "user": "airflow",
    "password": "admin",
    "database": "DWH",
    "port": 1433
}

def get_connection():
    return pymssql.connect(**DB_CONFIG)

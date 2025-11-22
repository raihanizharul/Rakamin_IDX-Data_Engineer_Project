/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

USE DWH;
GO

IF OBJECT_ID('bronze.transaction_excel_raw', 'U') IS NOT NULL
    DROP TABLE bronze.transaction_excel_raw;
GO

CREATE TABLE bronze.transaction_excel_raw(
    transaction_id      INT,
    account_id          INT,
    transaction_date    DATETIME2(0),
    amount              INT,
    transaction_type    NVARCHAR(50),
    branch_id           INT
);
GO

IF OBJECT_ID('bronze.transaction_csv_raw', 'U') IS NOT NULL
    DROP TABLE bronze.transaction_csv_raw;
GO

CREATE TABLE bronze.transaction_csv_raw(
    transaction_id      INT,
    account_id          INT,
    transaction_date    DATETIME2(0),
    amount              INT,
    transaction_type    NVARCHAR(50),
    branch_id           INT
);
GO

IF OBJECT_ID('bronze.transaction_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.transaction_db_raw;
GO

CREATE TABLE bronze.transaction_db_raw(
    transaction_id      INT,
    account_id          INT,
    transaction_date    DATETIME2(0),
    amount              INT,
    transaction_type    NVARCHAR(50),
    branch_id           INT
);
GO

IF OBJECT_ID('bronze.account_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.account_db_raw;
GO

CREATE TABLE bronze.account_db_raw(
    account_id      INT,
    customer_id     INT,
    account_type    NVARCHAR(50),
    balance         INT,
    date_opened     DATETIME2(0),
    status          NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.customer_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.customer_db_raw;
GO

CREATE TABLE bronze.customer_db_raw(
    customer_id     INT,
    customer_name   NVARCHAR(50),
    address         NVARCHAR(50),
    city_id         INT,
    age             INT,
    gender          NVARCHAR(50),
    email           NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.branch_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.branch_db_raw;
GO

CREATE TABLE bronze.branch_db_raw(
    branch_id       INT,
    branch_name     NVARCHAR(50),
    branch_location NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.city_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.city_db_raw;
GO

CREATE TABLE bronze.city_db_raw(
    city_id     INT,
    city_name   NVARCHAR(50),
    state_id    INT
);
GO

IF OBJECT_ID('bronze.state_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.state_db_raw;
GO

CREATE TABLE bronze.state_db_raw(
    state_id     INT,
    state_name   NVARCHAR(50),
);
GO
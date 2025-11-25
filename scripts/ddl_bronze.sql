/*
===============================================================================
DDL Script: Create Bronze Raw Tables 
===============================================================================
Script Purpose:
    Script ini digunakan untuk membuat tabel-tabel RAW pada layer Bronze 
    dalam Data Warehouse. Seluruh kolom NVARCHAR diseragamkan menjadi NVARCHAR(50)
    agar konsisten dan mudah dikelola.

Usage:
    - Digunakan sebagai landing area pertama dari berbagai sumber data.
===============================================================================
*/

USE DWH;
GO

-- =============================================================================
-- Create Table: bronze.transaction_excel_raw
-- =============================================================================

IF OBJECT_ID('bronze.transaction_excel_raw', 'U') IS NOT NULL
    DROP TABLE bronze.transaction_excel_raw;
GO

CREATE TABLE bronze.transaction_excel_raw (
    transaction_id      NVARCHAR(50),
    account_id          NVARCHAR(50),
    transaction_date    NVARCHAR(50),
    amount              NVARCHAR(50),
    transaction_type    NVARCHAR(50),
    branch_id           NVARCHAR(50)
);
GO

-- =============================================================================
-- Create Table: bronze.transaction_csv_raw
-- =============================================================================

IF OBJECT_ID('bronze.transaction_csv_raw', 'U') IS NOT NULL
    DROP TABLE bronze.transaction_csv_raw;
GO

CREATE TABLE bronze.transaction_csv_raw (
    transaction_id      NVARCHAR(50),
    account_id          NVARCHAR(50),
    transaction_date    NVARCHAR(50),
    amount              NVARCHAR(50),
    transaction_type    NVARCHAR(50),
    branch_id           NVARCHAR(50)
);
GO

-- =============================================================================
-- Create Table: bronze.transaction_db_raw
-- =============================================================================

IF OBJECT_ID('bronze.transaction_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.transaction_db_raw;
GO

CREATE TABLE bronze.transaction_db_raw (
    transaction_id      INT NOT NULL,
    account_id          INT,
    transaction_date    DATETIME2(0),
    amount              INT,
    transaction_type    NVARCHAR(50),
    branch_id           INT
);
GO

-- =============================================================================
-- Create Table: bronze.account_db_raw
-- =============================================================================

IF OBJECT_ID('bronze.account_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.account_db_raw;
GO

CREATE TABLE bronze.account_db_raw (
    account_id      INT NOT NULL,
    customer_id     INT,
    account_type    NVARCHAR(50),
    balance         INT,
    date_opened     DATETIME2(0),
    status          NVARCHAR(50)
);
GO

-- =============================================================================
-- Create Table: bronze.customer_db_raw
-- =============================================================================

IF OBJECT_ID('bronze.customer_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.customer_db_raw;
GO

CREATE TABLE bronze.customer_db_raw (
    customer_id     INT NOT NULL,
    customer_name   NVARCHAR(50),
    address         NVARCHAR(50),
    city_id         INT,
    age             NVARCHAR(50),
    gender          NVARCHAR(50),
    email           NVARCHAR(50)
);
GO

-- =============================================================================
-- Create Table: bronze.branch_db_raw
-- =============================================================================

IF OBJECT_ID('bronze.branch_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.branch_db_raw;
GO

CREATE TABLE bronze.branch_db_raw (
    branch_id       INT NOT NULL,
    branch_name     NVARCHAR(50),
    branch_location NVARCHAR(50)
);
GO

-- =============================================================================
-- Create Table: bronze.city_db_raw
-- =============================================================================

IF OBJECT_ID('bronze.city_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.city_db_raw;
GO

CREATE TABLE bronze.city_db_raw (
    city_id     INT NOT NULL,
    city_name   NVARCHAR(50),
    state_id    INT
);
GO

-- =============================================================================
-- Create Table: bronze.state_db_raw
-- =============================================================================

IF OBJECT_ID('bronze.state_db_raw', 'U') IS NOT NULL
    DROP TABLE bronze.state_db_raw;
GO

CREATE TABLE bronze.state_db_raw (
    state_id     INT NOT NULL,
    state_name   NVARCHAR(50)
);
GO

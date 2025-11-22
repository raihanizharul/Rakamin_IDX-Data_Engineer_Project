/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/

USE DWH;
GO

IF OBJECT_ID('silver.TransactionClean', 'U') IS NOT NULL
    DROP TABLE silver.TransactionClean;
GO

CREATE TABLE silver.TransactionClean(
    TransactionId      INT,
    AccountId          INT,
    TransactionDate    DATETIME2(0),
    Amount             INT,
    TransactionType    NVARCHAR(50),
    BranchId           INT
);
GO

IF OBJECT_ID('silver.AccountClean', 'U') IS NOT NULL
    DROP TABLE silver.AccountClean;
GO

CREATE TABLE silver.AccountClean(
    AccountId      INT,
    CustomerId     INT,
    AccountType    NVARCHAR(50),
    Balance        INT,
    DateOpened     DATETIME2(0),
    Status         NVARCHAR(50)
);
GO

IF OBJECT_ID('silver.CustomerClean', 'U') IS NOT NULL
    DROP TABLE silver.CustomerClean;
GO

CREATE TABLE silver.CustomerClean(
    CustomerId     INT,
    CustomerName   NVARCHAR(50),
    Address        NVARCHAR(50),
    CityId         INT,
    Age            INT,
    Gender         NVARCHAR(50),
    Email          NVARCHAR(50)
);
GO

IF OBJECT_ID('silver.BranchClean', 'U') IS NOT NULL
    DROP TABLE silver.BranchClean;
GO

CREATE TABLE silver.BranchClean(
    BranchId       INT,
    BranchName     NVARCHAR(50),
    BranchLocation NVARCHAR(50)
);
GO

IF OBJECT_ID('silver.CityClean', 'U') IS NOT NULL
    DROP TABLE silver.CityClean;
GO

CREATE TABLE silver.CityClean(
    CityId     INT,
    CityName   NVARCHAR(50),
    StateId    INT
);
GO

IF OBJECT_ID('silver.StateClean', 'U') IS NOT NULL
    DROP TABLE silver.StateClean;
GO

CREATE TABLE silver.StateClean(
    StateId     INT,
    StateName   NVARCHAR(50),
);
GO
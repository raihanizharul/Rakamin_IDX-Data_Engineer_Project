/*
===============================================================================
DDL Script: Create Silver Tables
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
    TransactionID      INT NOT NULL,
    AccountID          INT,
    TransactionDate    DATETIME2(0),
    Amount             INT,
    TransactionType    VARCHAR(50),
    BranchID           INT
);
GO

IF OBJECT_ID('silver.AccountClean', 'U') IS NOT NULL
    DROP TABLE silver.AccountClean;
GO

CREATE TABLE silver.AccountClean(
    AccountID      INT NOT NULL,
    CustomerID     INT,
    AccountType    VARCHAR(10),
    Balance        INT,
    DateOpened     DATETIME2(0),
    Status         VARCHAR(10)
);
GO

IF OBJECT_ID('silver.CustomerClean', 'U') IS NOT NULL
    DROP TABLE silver.CustomerClean;
GO

CREATE TABLE silver.CustomerClean(
    CustomerID     INT NOT NULL,
    CustomerName   VARCHAR(50),
    Address        VARCHAR(MAX),
    CityID         INT,
    Age            VARCHAR(3),
    Gender         VARCHAR(10),
    Email          VARCHAR(50)
);
GO

IF OBJECT_ID('silver.BranchClean', 'U') IS NOT NULL
    DROP TABLE silver.BranchClean;
GO

CREATE TABLE silver.BranchClean(
    BranchID       INT NOT NULL,
    BranchName     VARCHAR(50),
    BranchLocation VARCHAR(50)
);
GO

IF OBJECT_ID('silver.CityClean', 'U') IS NOT NULL
    DROP TABLE silver.CityClean;
GO

CREATE TABLE silver.CityClean(
    CityID     INT NOT NULL,
    CityName   VARCHAR(50),
    StateID    INT NOT NULL
);
GO

IF OBJECT_ID('silver.StateClean', 'U') IS NOT NULL
    DROP TABLE silver.StateClean;
GO

CREATE TABLE silver.StateClean(
    StateID     INT NOT NULL,
    StateName   VARCHAR(50),
);
GO
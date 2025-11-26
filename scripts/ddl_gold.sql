/*
===============================================================================
DDL Script: Create Gold Tables
===============================================================================
Script Purpose:
    This script creates Tables for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

Usage:
    - These tables can be queried directly for analytics and reporting.
===============================================================================
*/

USE DWH;
GO

-- =============================================================================
-- Create Dimension: gold.DimBranch
-- =============================================================================

IF OBJECT_ID('gold.DimBranch', 'U') IS NOT NULL
    DROP TABLE gold.DimBranch;
GO

CREATE TABLE gold.DimBranch (
    BranchID        INT PRIMARY KEY,
    BranchName      VARCHAR(50),
    BranchLocation  VARCHAR(50)
);
GO

-- =============================================================================
-- Create Dimension: gold.DimBranch 
--
-- (Created based on the join of Customer, City, and State)
-- =============================================================================

IF OBJECT_ID('gold.DimCustomer', 'U') IS NOT NULL
    DROP TABLE gold.DimCustomer;
GO

CREATE TABLE gold.DimCustomer (
    CustomerID    INT PRIMARY KEY,
    CustomerName  VARCHAR(50), 
    Address       VARCHAR(MAX), 
    CityName      VARCHAR(50), 
    StateName     VARCHAR(50), 
    Age           VARCHAR(3),
    Gender        VARCHAR(10), 
    Email         VARCHAR(50)
);
GO

-- =============================================================================
-- Create Dimension: gold.DimAccount
-- =============================================================================

IF OBJECT_ID('gold.DimAccount', 'U') IS NOT NULL
    DROP TABLE gold.DimAccount;
GO

CREATE TABLE gold.DimAccount (
    AccountID    INT  PRIMARY KEY,
    CustomerID   INT,
    AccountType  VARCHAR(10),
    Balance      INT,
    DateOpened   DATETIME2(0),
    Status       VARCHAR(10)

    -- Foreign Key Constraint ke DimCustomer
    CONSTRAINT FK_DimAccount_DimCustomer FOREIGN KEY (CustomerId) REFERENCES gold.DimCustomer(CustomerId)
);
GO

-- =============================================================================
-- Create Dimension: gold.FactTransaction
-- =============================================================================

IF OBJECT_ID('gold.FactTransaction', 'U') IS NOT NULL
    DROP TABLE gold.FactTransaction;
GO

CREATE TABLE gold.FactTransaction (
    TransactionID     INT PRIMARY KEY, 
    AccountID         INT,
    BranchID          INT,
    TransactionDate   DATETIME2(0),
    Amount            INT,
    TransactionType   VARCHAR(50),
    
    -- Foreign Key Constraints
    CONSTRAINT FK_FactTransaction_DimAccount FOREIGN KEY (AccountId) REFERENCES gold.DimAccount(AccountId),
    CONSTRAINT FK_FactTransaction_DimBranch FOREIGN KEY (BranchId) REFERENCES gold.DimBranch(BranchId)
);
GO

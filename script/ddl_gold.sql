/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
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
    BranchId        INT PRIMARY KEY,
    
    BranchName      NVARCHAR(50),
    BranchLocation  NVARCHAR(50)
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
    CustomerId    INT PRIMARY KEY,
    CustomerName  NVARCHAR(50), 
    Address       NVARCHAR(50), 
    CityName      NVARCHAR(50), 
    StateName     NVARCHAR(50), 
    Age           INT,
    Gender        NVARCHAR(50), 
    Email         NVARCHAR(50)
);
GO

-- =============================================================================
-- Create Dimension: gold.DimAccount
-- =============================================================================

IF OBJECT_ID('gold.DimAccount', 'U') IS NOT NULL
    DROP TABLE gold.DimAccount;
GO

CREATE TABLE gold.DimAccount (
    AccountId    INT  PRIMARY KEY,
    CustomerId   INT,
    AccountType  NVARCHAR(50),
    Balance      INT,
    DateOpened   DATETIME2(0),
    Status       NVARCHAR(50)

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
    TransactionId     INT PRIMARY KEY, 
    AccountId         INT,
    BranchId          INT,
    TransactionDate   DATETIME2(0),
    Amount            INT,
    TransactionType   NVARCHAR(50),
    
    -- Foreign Key Constraints
    CONSTRAINT FK_FactTransaction_DimAccount FOREIGN KEY (AccountId) REFERENCES gold.DimAccount(AccountId),
    CONSTRAINT FK_FactTransaction_DimBranch FOREIGN KEY (BranchId) REFERENCES gold.DimBranch(BranchId)
);
GO
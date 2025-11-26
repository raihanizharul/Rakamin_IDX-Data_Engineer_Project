USE DWH;
GO

IF OBJECT_ID('gold.usp_GetBalancePerCustomer', 'P') IS NOT NULL
    DROP PROCEDURE gold.usp_GetBalancePerCustomer;
GO

CREATE PROCEDURE gold.usp_GetBalancePerCustomer
    @name VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        c.CustomerName,
        a.AccountType,
        a.Balance AS InitialBalance,
        
        -- Hitung current balance
        a.Balance 
            + ISNULL(SUM(CASE WHEN t.TransactionType = 'Deposit' 
                              THEN t.Amount 
                              ELSE 0 END), 0)
            - ISNULL(SUM(CASE WHEN t.TransactionType <> 'Deposit' 
                              THEN t.Amount 
                              ELSE 0 END), 0)
            AS CurrentBalance

    FROM gold.DimAccount a
    INNER JOIN gold.DimCustomer c 
        ON a.CustomerID = c.CustomerID
    LEFT JOIN gold.FactTransaction t
        ON a.AccountID = t.AccountID

    WHERE a.Status = 'Active'
      AND c.CustomerName LIKE '%' + @name + '%'

    GROUP BY 
        c.CustomerName,
        a.AccountType,
        a.Balance;
END;
GO

/*
    EXEC gold.usp_GetBalancePerCustomer 'shelly';
*/

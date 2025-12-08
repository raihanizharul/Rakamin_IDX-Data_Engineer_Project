USE DWH;
GO

IF OBJECT_ID('gold.usp_GetDailyTransaction', 'P') IS NOT NULL
    DROP PROCEDURE gold.usp_GetDailyTransaction;
GO

CREATE PROCEDURE gold.usp_GetDailyTransaction
    @start_date DATE,
    @end_date   DATE
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        CAST(TransactionDate AS DATE) AS [Date],
        COUNT(*) AS TotalTransactions,
        SUM(Amount) AS TotalAmount
    FROM gold.FactTransaction
    WHERE CAST(TransactionDate AS DATE) BETWEEN @start_date AND @end_date
    GROUP BY CAST(TransactionDate AS DATE)
    ORDER BY [Date];
END;
GO


/*
    EXEC gold.usp_GetDailyTransaction 
     @start_date = '2024-01-18',
     @end_date   = '2024-01-20';
*/

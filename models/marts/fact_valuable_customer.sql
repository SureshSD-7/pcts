WITH customer_transactions AS (
    SELECT
        c.CustomerID,
        CONCAT(c.FirstName, ' ', c.LastName) as CustomerName,
        t.TOTALAMOUNT
    FROM {{ ref('stg_customer') }} c
    JOIN {{ ref('stg_transaction') }} t ON c.CustomerID = t.CustomerID
)

SELECT
    CustomerID,
    CustomerName,
    ROUND(SUM(TOTALAMOUNT),3) AS TotalTransactionAmount
FROM customer_transactions
GROUP BY CustomerID, CustomerName
ORDER BY TotalTransactionAmount DESC

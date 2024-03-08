WITH products AS (
    SELECT 
        ProductID,
        ProductName
    FROM {{ ref('stg_products') }}
), customers AS (
    SELECT
        CustomerID,
        CONCAT(FirstName, ' ', LastName) AS FullName,
        Email,
        Phone
    FROM {{ ref('stg_customer') }} 
), transactions AS (
    SELECT
        TransactionID,
        CustomerID,
        ProductID,
	Quantity,
	TotalAmount,
	TransactionDate
    FROM {{ ref('stg_transaction') }}
)

SELECT 
    t.TransactionID,
    c.CustomerID,
    c.FullName,
    c.Email,
    c.Phone,
    p.ProductID,
    p.ProductName,
    t.Quantity,
    t.TotalAmount,
    t.TransactionDate
FROM transactions t
JOIN customers c ON t.CustomerID = c.CustomerID
JOIN products p ON t.ProductID = p.ProductID
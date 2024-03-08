WITH product_availability AS (
    SELECT
        p.ProductID,
        p.ProductName,
        p.StockQuantity,
        s.StockOnHand
    FROM {{ ref('stg_products') }} p
    LEFT JOIN {{ ref('stg_stocks') }} s ON p.ProductID = s.ProductID
)

SELECT
    ProductID,
    ProductName,
    StockQuantity,
    StockOnHand,
    CASE
        WHEN StockOnHand > 0 THEN 'In Stock'
        ELSE 'Out of Stock'
    END AS Availability
FROM product_availability

with products as(
    SELECT 
        ProductID,
        ProductName,
        StockQuantity
    FROM {{ ref('stg_products') }}
), stocks as (
    SELECT
        ProductID,
        StockOnHand,
        REORDERLEVEL
    FROM {{ ref('stg_stocks') }}
),

final AS (
    SELECT 
        p.ProductID,
        p.ProductName,
        p.StockQuantity,
        s.StockOnHand,
        s.REORDERLEVEL
    FROM products p
    LEFT JOIN stocks s ON p.ProductID = s.ProductID
)

SELECT * FROM final

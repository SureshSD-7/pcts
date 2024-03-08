with products as(
    SELECT * FROM {{ source('DATA', 'Product') }}
)

SELECT * FROM products
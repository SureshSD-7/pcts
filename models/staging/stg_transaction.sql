with transactions as(
    SELECT * FROM {{ source('DATA', 'Transaction') }}
)

SELECT * FROM transactions
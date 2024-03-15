-- WITH split_data AS (
-- SELECT
-- Data
-- FROM {{ ref('stg_text_data') }}
-- )
-- SELECT
-- *
-- FROM split_data
-- models/split_data_model.sql
-- CASE WHEN REGEXP_INSTR(Data, '[0-9]') > 0
-- THEN REGEXP_SUBSTR(Data, '([0-9]+)')::INT
-- ELSE NULL
-- END AS id,
-- Use the source table
with
    raw_data as (select data from {{ ref("stg_text_data") }}),

    -- Split the string into columns using regular expressions
    split_data as (
        select
            data,
            try_cast(regexp_substr(data, '\\d*') as integer) as id,
            regexp_substr(data, '[a-zA-Z]+') as name,
            case
                when id is null
                then
                    try_cast(
                        substring(
                            regexp_substr(data, '\\d+'),
                            1,
                            length(regexp_substr(data, '\\d+')) - 2
                        ) as integer
                    )
                else
                    try_cast(
                        substring(
                            regexp_substr(data, '\\d+', 1, 2),
                            1,
                            length(regexp_substr(data, '\\d+', 1, 2)) - 2
                        ) as integer
                    )
            end as salary,
            TRY_CAST(TO_DATE(REGEXP_SUBSTR(data, '[0-9]{2}-[0-9]{2}-[0-9]{4}'), 'DD-MM-YYYY') AS DATE) AS start_date,
            TRY_CAST(TO_DATE(REGEXP_SUBSTR(data, '[0-9]{2}-[0-9]{2}-[0-9]{4}', 1, 2), 'DD-MM-YYYY') AS DATE) AS end_date,
        from raw_data
    )

-- Store the results in a new table
select *
from split_data


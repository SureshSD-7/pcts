with
    raw_data as (select data from {{ ref("stg_text_data") }}),

    -- Split the string into columns using regular expressions
    split_data_trans as (
        select
            data,
            regexp_substr(data, '[a-zA-Z]+') as name,
            case
                when name is null
                then try_cast(regexp_substr(data, '\\d{4}') as integer)
                else try_cast(regexp_substr(data, '\\d*') as integer)
            end as id,
            case 
            when name is null 
            then          
                TRY_CAST(
                    SUBSTRING(
                        regexp_substr(data, '\\d+'),
                        5,
                        LENGTH(regexp_substr(data, '\\d+')) - 6
                    ) AS INTEGER
                )
            else 
                CASE 
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
                END
            end as salary,
            TRY_CAST(TO_DATE(REGEXP_SUBSTR(data, '[0-9]{2}-[0-9]{2}-[0-9]{4}'), 'DD-MM-YYYY') AS DATE) AS start_date,
            TRY_CAST(TO_DATE(REGEXP_SUBSTR(data, '[0-9]{2}-[0-9]{2}-[0-9]{4}', 1, 2), 'DD-MM-YYYY') AS DATE) AS end_date,
        from raw_data
    )

-- Store the results in a new table
select *
from split_data_trans


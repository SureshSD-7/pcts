SELECT
    ID,
    NAME,
    SALARY,
    START_DATE, 
    END_DATE
FROM {{ ref('int_text_transform') }}
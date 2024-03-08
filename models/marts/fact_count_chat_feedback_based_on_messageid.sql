with response as (
    select count(*) as no_of_response from {{ ref("stg_chat") }}
    ),
    feedback as (
SELECT
    (SELECT COUNT(*) FROM {{ ref("stg_feedback") }} WHERE SCORE = 1) as positive_count,
    (SELECT COUNT(*) FROM {{ ref("stg_feedback") }} WHERE SCORE = 0) as negative_count
    ),
    final_table as (
        select 
           r.no_of_response as TotalResponse,
           f.negative_count,
           f.positive_count,
           (r.no_of_response - COALESCE(f.negative_count, 0) - COALESCE(f.positive_count, 0)) AS No_Response
        from response r
        left JOIN feedback f
        )

select *
from final_table


           
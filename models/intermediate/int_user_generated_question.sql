With user_generate_quest as(
    SELECT
    json_data:generated_question.intermediate_llm_prompt:: STRING as intermediate_llm_prompt,
    json_data:generated_question.intermediate_llm_response:: STRING as intermediate_llm_response,
    json_data:generated_question.cost:: STRING as cost,
    MD5(json_data:user_context.session_id::STRING) as session_id_hk
    FROM {{ ref('stg_chat_data') }}

)

SELECT * FROM user_generate_quest
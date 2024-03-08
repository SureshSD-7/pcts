With user_interaction as(
    SELECT
    json_data:message_id:: STRING as message_id,
    json_data:timestamp:: STRING as timestamp,
    json_data:user_prompt:: STRING as user_prompt,
    json_data:llm_prompt:: STRING as llm_prompt,
    json_data:llm_response:: STRING as llm_response,
    json_data:cost:: STRING as llm_cost,
    json_data:exception:: STRING as exception,
    MD5(json_data:user_context.session_id::STRING) as session_id_hk
    FROM {{ ref('stg_chat_data') }}

)

SELECT * FROM user_interaction
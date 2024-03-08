With user_info as(
    SELECT
    json_data:message_id:: STRING as message_id,
    json_data:user_context.name::STRING as name,
    json_data:user_context.session_id::STRING as session_id,
    MD5(json_data:user_context.session_id::STRING) as session_id_hk
    FROM {{ ref('stg_chat_data') }}

)

SELECT * FROM user_info
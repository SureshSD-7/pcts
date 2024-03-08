-- user_generate_quest.sql

WITH user_source_doc AS (
    SELECT
        f.value:page_content::STRING AS page_content,
        f.value:metadata.id::STRING AS metadata_id,
        f.value:type::STRING AS type,
        MD5(json_data:user_context.session_id::STRING) AS session_id_hk,
        json_data:message_id:: STRING as message_id
    FROM {{ ref('stg_chat_data') }},
         LATERAL FLATTEN(input => json_data:source_docs_retrieved) AS f 
)
--FLATTEN function to unnest the source_docs_retrieved array, creating a separate row for each element in the array

SELECT *
FROM user_source_doc

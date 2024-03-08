With user_metadata_info as(
    SELECT
        f.value:metadata.id::STRING AS metadata_id,
        f.value:metadata.source::STRING AS source,
        f.value:metadata.file_path::STRING AS file_path,
        f.value:metadata.page::INT AS page,
        f.value:metadata.total_pages::INT AS total_pages,
        f.value:metadata.Author::STRING AS Author,
        f.value:metadata.CreationDate::STRING AS CreationDate,
        f.value:metadata.ModDate::STRING AS ModDate,
        f.value:metadata.Producer::STRING AS Producer,
        f.value:metadata.Title::STRING AS Title
    FROM {{ ref('stg_chat_data') }},
         LATERAL FLATTEN(input => json_data:source_docs_retrieved) AS f 
)

SELECT * FROM user_metadata_info
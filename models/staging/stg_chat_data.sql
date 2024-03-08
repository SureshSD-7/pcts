with Chat as (select * from {{ source("CHAT_SCHEMA", "CHATFX") }}) 


select * from Chat
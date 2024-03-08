with customer as (select * from {{ source("DATA", "CUSTOMER") }}) 


select * from customer

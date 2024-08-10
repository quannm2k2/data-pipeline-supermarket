with source as (
    select * from {{ ref('stg_supermarket__departments') }}
)

select * from source
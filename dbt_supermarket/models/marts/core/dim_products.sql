with source as (
    select * from {{ ref('stg_supermarket__products') }}
)

select * from source
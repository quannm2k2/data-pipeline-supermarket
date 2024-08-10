with source as (
    select * from {{ source('src_supermarket', 'aisles') }}
),
renamed as (
    select
        cast(aisle_id as integer) as aisle_id,
        aisle
    from source
)

select * from renamed
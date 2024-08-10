with source as (
    select * from {{ source('src_supermarket', 'products') }}
),
renamed as (
    select
        cast(product_id as integer) as product_id,
        product_name,
        cast(aisle_id as integer) as aisle_id,
        cast(department_id as integer) as department_id
    from source
)

select * from renamed
with source as (
    select * from {{ source('src_supermarket', 'order_products') }}
),
renamed as (
    select
        cast(order_id as integer) as order_id,
        cast(product_id as integer) as product_id,
        cast(add_to_cart_order as integer) as add_to_cart_order,
        cast(reordered as integer) as reordered
    from source
)

select * from renamed
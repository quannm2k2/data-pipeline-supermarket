with source as (
    select * from {{ source('src_supermarket', 'orders_small_version') }}
),
renamed as (
    select
        CAST(order_id AS INTEGER) AS order_id,
        CAST(user_id AS INTEGER) AS user_id,
        eval_set,
        CAST(order_number AS INTEGER) AS order_number,
        CAST(order_dow AS INTEGER) AS order_dow,
        CAST(order_hour_of_day AS INTEGER) AS order_hour_of_day,
        CASE
            WHEN days_since_prior_order = null THEN NULL
            ELSE CAST(days_since_prior_order AS FLOAT)
        END AS days_since_prior_order
    from source
)

select * from renamed
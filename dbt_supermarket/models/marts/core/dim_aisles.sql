-- Logic không thay đổi từ Staging lên (có thể bỏ đi)

with source as (
    select * from {{ ref('stg_supermarket__aisles') }}
)

select * from source
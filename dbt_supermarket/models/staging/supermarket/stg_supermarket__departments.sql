with source as (
    select * from {{ source('src_supermarket', 'departments') }}
),
renamed as (
    select
        cast(department_id as integer) as department_id,
        department
    from source
)

select * from renamed
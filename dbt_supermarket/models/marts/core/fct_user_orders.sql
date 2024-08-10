select 
    os.user_id,
    op.*
from 
    {{ ref('stg_supermarket__order_products') }} op
join
    {{ ref('stg_supermarket__orders_small_version') }} os
on
    op.order_id = os.order_id
order by
    os.user_id, op.add_to_cart_order

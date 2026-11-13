select
    product_id,
    product_name,
    category,
    sum(quantity)                as units_sold,
    {{ round_half_up('sum(line_revenue)') }} as revenue
from {{ ref('stg_successful_orders') }}
group by product_id, product_name, category

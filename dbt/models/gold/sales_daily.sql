select
    order_date                                             as date,
    count(distinct order_id)                               as total_orders,
    {{ round_half_up('sum(line_revenue)') }} as total_revenue,
    {{ round_half_up(round_half_up('sum(line_revenue)') ~ ' / count(distinct order_id)') }} as average_order_value
from {{ ref('stg_successful_orders') }}
group by order_date

-- « region » = pays : le niveau le plus lisible en BI pour ce jeu de données international.
select
    country                                                as region,
    count(distinct order_id)                               as orders,
    {{ round_half_up('sum(line_revenue)') }} as revenue,
    {{ round_half_up(round_half_up('sum(line_revenue)') ~ ' / count(distinct order_id)') }} as average_order_value
from {{ ref('stg_successful_orders') }}
group by country

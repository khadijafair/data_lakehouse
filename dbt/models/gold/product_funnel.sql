-- Entonnoir de conversion par produit : vues -> ajouts au panier -> commandes.
-- Croise les deux modes d'ingestion du lakehouse :
--   * clickstream : temps réel (Kafka -> Spark Structured Streaming -> Bronze -> Silver)
--   * commandes   : batch (CSV -> Spark -> Bronze -> Silver)
-- Calculé sur la période commune aux deux sources (vars funnel_start_date / funnel_end_date).
-- Données synthétiques : les taux décrivent la mécanique, pas un vrai comportement client.

with clicks as (
    select
        product_id,
        count_if(event_type = 'page_view')   as views,
        count_if(event_type = 'add_to_cart') as add_to_carts
    from {{ source('silver', 'clickstream') }}
    where product_id is not null
      and cast(event_ts as date) between date '{{ var("funnel_start_date") }}'
                                     and date '{{ var("funnel_end_date") }}'
    group by product_id
),

purchases as (
    select
        product_id,
        count(distinct order_id) as orders,
        sum(quantity)            as units_sold
    from {{ ref('stg_successful_orders') }}
    where order_date between date '{{ var("funnel_start_date") }}'
                         and date '{{ var("funnel_end_date") }}'
    group by product_id
)

select
    p.product_id,
    p.product_name,
    p.category,
    coalesce(c.views, 0)        as views,
    coalesce(c.add_to_carts, 0) as add_to_carts,
    coalesce(o.orders, 0)       as orders,
    coalesce(o.units_sold, 0)   as units_sold,
    -- taux en double, arrondis à 4 décimales ; null si le dénominateur vaut 0
    {{ round_half_up('cast(c.add_to_carts as double) / nullif(c.views, 0)', 4) }}  as view_to_cart_rate,
    {{ round_half_up('cast(o.orders as double) / nullif(c.add_to_carts, 0)', 4) }} as cart_to_order_rate,
    {{ round_half_up('cast(o.orders as double) / nullif(c.views, 0)', 4) }}        as view_to_order_rate
from {{ source('silver', 'products') }} p
left join clicks c on c.product_id = p.product_id
left join purchases o on o.product_id = p.product_id

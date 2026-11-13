-- Les totaux de l'entonnoir doivent correspondre exactement aux événements Silver
-- de la période (aucun événement perdu ni compté deux fois par la jointure).
with expected as (
    select
        count_if(event_type = 'page_view')   as views,
        count_if(event_type = 'add_to_cart') as add_to_carts
    from {{ source('silver', 'clickstream') }}
    where product_id in (select product_id from {{ source('silver', 'products') }})
      and cast(event_ts as date) between date '{{ var("funnel_start_date") }}'
                                     and date '{{ var("funnel_end_date") }}'
),
actual as (
    select sum(views) as views, sum(add_to_carts) as add_to_carts
    from {{ ref('product_funnel') }}
)
select e.views, a.views, e.add_to_carts, a.add_to_carts
from expected e cross join actual a
where e.views <> a.views or e.add_to_carts <> a.add_to_carts

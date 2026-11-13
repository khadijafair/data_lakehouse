-- Base commune aux 4 tables Gold.
-- Décision métier : seules les commandes 'success' génèrent du chiffre d'affaires.
select
    order_id,
    order_date,
    customer_id,
    product_id,
    product_name,
    category,
    country,
    quantity,
    order_amount * quantity as line_revenue
from {{ source('silver', 'orders_enriched') }}
where status = 'success'

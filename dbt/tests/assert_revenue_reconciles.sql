-- Les 4 tables Gold doivent donner le même chiffre d'affaires total
-- (à 1 unité près, à cause des arrondis à 2 décimales ligne par ligne).
-- Le test échoue s'il renvoie au moins une ligne.
with totals as (
    select 'sales_daily' as source_table, sum(total_revenue) as revenue from {{ ref('sales_daily') }}
    union all
    select 'sales_by_product', sum(revenue) from {{ ref('sales_by_product') }}
    union all
    select 'sales_by_region', sum(revenue) from {{ ref('sales_by_region') }}
    union all
    select 'customer_metrics', sum(total_spent) from {{ ref('customer_metrics') }}
)
select source_table, revenue
from totals
where abs(revenue - (select max(revenue) from totals)) > 1

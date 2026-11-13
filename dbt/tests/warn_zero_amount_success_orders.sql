-- Avertissement (n'arrête pas le pipeline) : commandes « success » dont le montant vaut 0.
-- Le Silver accepte order_amount >= 0, or 12 988 commandes sources ont « 0 » comme montant
-- (valeur probablement sentinelle, comme les dates). Elles comptent comme commandes
-- mais pas dans le chiffre d'affaires, et font baisser le panier moyen.
-- Décision à prendre : les rejeter en Silver (règle order_amount > 0) ou les garder.
{{ config(severity='warn') }}

select order_id, customer_id, order_date
from {{ source('silver', 'orders_enriched') }}
where status = 'success'
  and order_amount = 0

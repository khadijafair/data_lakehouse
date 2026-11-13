{#
  Arrondi « au demi supérieur » calculé en décimal, comme Spark.
  round() de Trino sur un double donne parfois un centime d'écart
  (ex. 12.345 stocké 12.34499999... en binaire). Passer par un decimal
  donne exactement les mêmes valeurs que l'ancienne couche Gold Spark.
#}
{% macro round_half_up(expr, scale=2) -%}
cast(round(cast({{ expr }} as decimal(38, 12)), {{ scale }}) as double)
{%- endmacro %}

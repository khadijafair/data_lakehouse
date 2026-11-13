-- Test générique : un taux doit être compris entre 0 et 1 (null autorisé si dénominateur nul).
{% test rate_between_0_and_1(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and not ({{ column_name }} between 0 and 1)
{% endtest %}

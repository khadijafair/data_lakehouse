-- Test générique : la colonne doit être positive ou nulle, et finie (NaN échoue).
{% test non_negative(model, column_name) %}
select {{ column_name }}
from {{ model }}
where not ({{ column_name }} >= 0)
{% endtest %}

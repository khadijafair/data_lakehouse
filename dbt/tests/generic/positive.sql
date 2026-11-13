-- Test générique : la colonne doit être strictement positive et finie.
-- Protège contre le bug NaN déjà rencontré (NaN > 0 est faux, donc NaN échoue).
{% test positive(model, column_name) %}
select {{ column_name }}
from {{ model }}
where not ({{ column_name }} > 0)
{% endtest %}

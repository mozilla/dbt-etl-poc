{% test equals(model, actual, expected) %}

select *
from (select "error")
where
    {{ expected }} != {{ actual }}

{% endtest %}

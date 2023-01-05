-- materialization to create UDFs

{% materialization udf, adapter='bigquery' %}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- set target_relation = this %}
    {{- run_hooks(pre_hooks) -}} 
    {%- call statement('main') -%}
      {{ sql }}
    {%- endcall -%}
    {{ run_hooks(post_hooks) }}  
    {% do persist_docs(target_relation, model) %} 
    {{ return({'relations': [target_relation]  }) }}
{% endmaterialization %}

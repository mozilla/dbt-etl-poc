-- from https://medium.com/tiket-com/creating-append-materialization-in-dbt-1469c8beafc3

{% materialization append_only, adapter='bigquery' %}
    {%- set target_relation = this %}
    {%- set partition_by = adapter.parse_partition_by(config.get('partition_by', none)) -%}
    {%- set cluster_by = config.get('cluster_by', none) -%}
    {%- set tmp_identifier = "temp_" ~ target_relation.identifier %}
    {%- set tmp_relation = target_relation.incorporate(path=    
        {"identifier": tmp_identifier, "schema": config.get('temp_schema', default=target_relation.schema)}
    ) -%}
    {%- set existing_relation = load_relation(this) -%}    
    {% if existing_relation is none or should_full_refresh() %}
        {%- set build_sql = build_append_only_initial_sql(target_relation, tmp_relation) %}
    {% else %}
        {%- set build_sql = build_append_only_sql(target_relation, tmp_relation) %}    
    {% endif %}
    {{- run_hooks(pre_hooks) -}}   
    {%- call statement('main') -%}
        {{ build_sql }}
    {% endcall %}    
    {{ run_hooks(post_hooks) }}    
    {% set target_relation = this.incorporate(type='table') %}    
    {% do persist_docs(target_relation, model) %}    
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}

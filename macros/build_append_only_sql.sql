{%- macro build_append_only_sql(target_relation, temp_relation) -%}    
    {%- set columns = adapter.get_columns_in_relation(target_relation) -%}    
    {%- set csv_colums = get_quoted_csv(columns | map(attribute="name")) %}    
    {{ create_table_as(True, temp_relation, sql) }}    
    INSERT {{ target_relation }} ({{ csv_colums }})
    SELECT DISTINCT
    *, CURRENT_TIMESTAMP() as processed_timestamp
    FROM
    {{ temp_relation }}
{%- endmacro -%}

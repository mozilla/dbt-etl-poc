{%- macro build_append_only_initial_sql(target_relation, temp_relation) -%}    
    {{ create_table_as(True, temp_relation, sql) }}
    {%- set initial_sql -%}
        SELECT
          * ,CURRENT_TIMESTAMP() as processed_timestamp
        FROM
          {{ temp_relation }}
    {%- endset -%}    
    {{ create_table_as(False, target_relation, initial_sql) }}
{%- endmacro -%}

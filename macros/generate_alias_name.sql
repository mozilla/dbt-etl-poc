{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name is none -%}

        {% set node_name = node.name %}
        {% set split_name = node_name.split('__') %}
        {{ split_name[-1] | trim }}

    {%- else -%}

        {{ custom_alias_name | trim }}

    {%- endif -%}

{%- endmacro %}

{#- /* get data-type of given value */ -#}
{%- macro get_value_data_type(value) -%}
    {%- if value is integer -%}
        {%- set field_type = 'NUMBER' -%}
    {%- elif value is float -%}
        {%- set field_type = 'FLOAT' -%}
    {%- elif value is boolean -%}
        {%- set field_type = 'BOOLEAN' -%}
    {%- else -%}
        {%- set field_type = 'VARCHAR' -%}
    {%- endif -%}
    {{return(field_type)}}
{%- endmacro -%}
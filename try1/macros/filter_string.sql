{#- /* filter_string */ -#}
{%- macro filter_string(filter_string, filter_value, replace_value='') -%}
    {%- if filter_value in filter_string -%}
        {{filter_string.replace(filter_value, replace_value)}}
    {%- else -%}
        {{filter_string}}
    {%- endif -%}
{%- endmacro -%}
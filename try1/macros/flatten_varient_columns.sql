{#- /* Flatten Varient columns in given table */ -#}
{%- macro flatten_varient_columns(table_name, len_sampleing_records=10, keep_varient_columns=false) -%}
    {%- if execute -%}
        {#- /* get all columns from that table */ -#}
        {%- set columns = adapter.get_columns_in_relation(table_name) -%}
        {%- set column_names=[] -%}
        {%- for column in columns -%}
            {%- do column_names.append(column['column']|lower) -%}
        {%- endfor-%}
        {%- set found_columns=[] -%}
        {%- set ignored_columns=[] -%}
        {% set results = run_query('SELECT * FROM ' ~ table_name ~ ' LIMIT ' ~ len_sampleing_records) %}
        {%- for column in columns -%}
            {%- set column_name = column['column'] -%}
            {%- set column_dtype = column['dtype'] -%}
            {%- set column_is_sdc = '_SDC_' in column_name -%}
            {%- set properties_column = column_name == 'PROPERTIES' -%}
            {%- set wanted_column = not (column_is_sdc or properties_column) -%}
            {#- {%- do log(loop.index~"/"~loop.length ~ " column_name: " ~ column_name ~ "; column_dtype: " ~ column_dtype ~ "; column_is_sdc: " ~ column_is_sdc, info=true) -%} -#}
            {#- /* ignore columns starting with _SDC_; i think these are stitch-generated columns and we won't be needing these */ -#}
            {%- if wanted_column -%}
                {%- set parsed_column = parse_varient_column(column_name, column_dtype, loop.index0, results, column_names, keep_varient_columns=keep_varient_columns) -%}
                {%- for found_column in parsed_column[0] -%}
                    {%- if found_column|lower not in ['from'] -%}
                        {%- do found_columns.append(found_column) -%}
                    {%- endif -%}
                {%- endfor -%}
                {%- for ignored_column in parsed_column[1] -%}
                    {%- do ignored_columns.append(ignored_column) -%}
                {%- endfor -%}
            {%- else -%}
                {#- {%- do ignored_columns.append(column_name) -%} -#}
                {#- {%- do log("ignoring SDC column...", info=true) -%} -#}
            {%- endif -%}
        {%- endfor -%}
        {#- /* generating sql query from all new columns */ -#}
        {%- for found_column in found_columns %}
    {{found_column}}
            {%- if loop.index!=loop.length -%}
                ,
            {%- endif -%}
        {% endfor -%}
        {%- do log("Flattened " ~ column_names|length ~ " columns from " ~ table_name ~ " to " ~ found_columns|length ~ " columns while ignoring " ~ ignored_columns|length ~ " columns.", info=true) -%}
    {%- endif -%}
{%- endmacro -%}
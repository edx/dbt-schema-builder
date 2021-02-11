{#- /* Flatten Varient columns in given table */ -#}
{%- macro parse_varient_column(column_name, column_dtype, column_number, results, column_names, keep_varient_columns=false) -%}
    {%- set found_columns=[] -%}
    {%- set ignored_columns={} -%}
    {#- /* check if it's a varient column */ -#}
    {%- if column_dtype == 'VARIANT' -%}
        {#- /* get some records from table to get an idea of possiable field in that structure*/ -#}
        {% set records = results.columns[column_number].values() %}
        {%- set field_names = {} -%}
        {%- for record in records -%}
            {%- if record is not none -%}
                {%- set parsed_record = fromjson(record) -%}
                {#- /* see if it's a dict of a list */ -#}
                {%- if parsed_record is mapping -%}
                    {#- /* if it's a dict then add collect all unique field names across all gathered records */ -#}
                    {%- for key, value in parsed_record.items() -%}
                        {#- /* check if it's dict or list or anyother value*/ -#}
                        {%- if value is none -%}
                        {%- elif value is mapping -%}
                        {%- elif ((value is sequence) and (value is not string)) -%}
                        {%- else -%}
                            {#- /* if it's neither mapping nor list then add it to field_names to be created a seprate column of*/ -#}
                            {#- /* parse data type of this column */-#}
                            {%- set field_type = get_value_data_type(value) -%}
                            {#- /* add this to field_names */ -#}
                            {#- /* if not already in fields_name add this data type */ -#}
                            {%- if key not in field_names -%}
                                {%- do field_names.update({key:field_type}) -%}
                            {#- /* if existing data-type is VARCHAR then let it be */ -#}
                            {%- elif field_names[key]=='VARCHAR' -%}
                            {#- /* if current data-type is not same as already set data-type, then set it to VARCHAR */ -#}
                            {%- elif field_names[key]!=field_type -%}
                                {%- do field_names.update({key:'VARCHAR'}) -%}
                            {%- endif -%}
                        {%- endif -%}
                    {%- endfor -%}
                {%- elif parsed_record is sequence -%}
                    {%- if column_name not in ignored_columns -%}
                        {%- do ignored_columns.update({column_name:'LIST TYPE COLUMN'}) -%}
                    {%- endif -%}
                    {# {%- do log("varient of type list not handeled yet! ignoring and moving on...", info=true) -%} #}
                {%- else -%}
                    {%- if column_name not in ignored_columns -%}
                        {%- do ignored_columns.update({column_name:'UN-KNOWN TYPE COLUMN'}) -%}
                    {%- endif -%}
                    {# {%- do log("varient of un-known type encountered! ignoring and moving on..." ~ record, info=true) -%} #}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
        {%- for list_column, reason in ignored_columns.items() -%}
            {%- do log("Ignoring Column: " ~ table_name ~ "." ~ list_column ~ " because " ~ reason, info=true) -%}
        {%- endfor -%}
        {%- if field_names -%}
            {#- /* check if field names contains a field named value */ -#}
            {%- if 'value' in field_names -%}
                {#- /* if it does then map this value field as seprate column*/ -#}
                {%- if keep_varient_columns -%}
                    {%- set field_column_name_as = filter_string(column_name ~ "_value", 'PROPERTY_') -%}
                {%- else -%}
                    {%- set field_column_name_as = filter_string(column_name, 'PROPERTY_') -%}
                {%- endif -%}
                {%- set field_column_name = "NULLIF(json_extract_path_text(" ~ column_name ~ ", 'value'), '')::" ~ field_names['value'] ~ " as " ~ field_column_name_as|upper -%}
                {%- if field_column_name_as|lower not in column_names -%}
                    {%- do found_columns.append(field_column_name) -%}
                {%- endif -%}
            {%- else -%}
                {#- /* otherwise, map all field names as seprate columns */ -#}
                {%- for field_name, field_type in field_names.items() -%}
                    {%- if '-' not in field_name -%}
                        {%- set field_column_name_as = filter_string(column_name ~ "_" ~ field_name, 'PROPERTY_') -%}
                        {%- set field_column_name = "NULLIF(json_extract_path_text(" ~ column_name ~ ", '" ~ field_name ~ "'), '')::" ~ field_type ~ " as " ~ field_column_name_as|upper -%}
                        {%- if field_column_name_as|lower not in column_names -%}
                            {%- do found_columns.append(field_column_name) -%}
                        {%- endif -%}
                    {%- endif -%}
                {%- endfor -%}
            {%- endif -%}
        {%- endif -%}
        {#- /* adding this line to keep original non-flatened column */ -#}
        {%- if keep_varient_columns -%}
            {%- do found_columns.append(column_name) -%}
        {%- endif -%}
    {%- else -%}
        {#- /* adding non-varient columns as is */ -#}
        {%- do found_columns.append(column_name) -%}
    {%- endif -%}
    {{ return([found_columns, ignored_columns]) }}
{%- endmacro -%}
{%- set TABLE_NAME="ENGAGEMENTS" -%}
{%- set FULL_TABLE_NAME="PROD.HUBSPOT_STITCH_RAW."~TABLE_NAME -%}
select {{flatten_varient_columns(FULL_TABLE_NAME)}}
from {{FULL_TABLE_NAME}}

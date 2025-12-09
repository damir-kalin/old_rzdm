{% macro mysql__get_drop_sql(relation) -%}
  {%- if relation is none -%}
    {{ return("") }}
  {%- endif -%}
  {%- set relation_type = relation.type -%}
  {%- if relation_type == 'view' -%}
    {%- set drop_command = 'drop view if exists' -%}
  {%- elif relation_type == 'table' -%}
    {%- set drop_command = 'drop table if exists' -%}
  {%- else -%}
    {%- set drop_command = 'drop ' ~ relation_type ~ ' if exists' -%}
  {%- endif -%}
  {%- set full_relation = relation.render() -%}
  {{ drop_command }} {{ full_relation }}
{%- endmacro %}


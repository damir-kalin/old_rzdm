{% macro mysql__drop_relation(relation) -%}
  {%- if relation is none -%}
    {{ return("") }}
  {%- endif -%}
  -- For StarRocks: DROP without CASCADE (StarRocks doesn't support CASCADE)
  {%- set relation_type = relation.type -%}
  {%- if relation_type == 'view' -%}
    drop view if exists {{ relation }}
  {%- else -%}
    drop table if exists {{ relation }}
  {%- endif -%}
{%- endmacro %}


{% macro starrocks_source_relation(source_name, table_name) %}
  {#
    Кастомный макрос для StarRocks, где схема = база данных.
    Формирует путь как database.table вместо database.schema.table
  #}
  {%- set source_relation = source(source_name, table_name) -%}
  {%- set database = source_relation.database or target.database -%}
  {{ adapter.quote(database) }}.`{{ table_name }}`
{% endmacro %}

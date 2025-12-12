{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
  {#
    Переопределяем логику генерации имени схемы для MySQL/StarRocks.
    Возвращаем схему напрямую из custom_schema_name без добавления префиксов.
  #}
  {%- if custom_schema_name is not none -%}
    {{ return(custom_schema_name) }}
  {%- else -%}
    {{ return(target.schema) }}
  {%- endif -%}
{%- endmacro %}


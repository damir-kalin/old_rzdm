{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
  {#
    Фиксируем схему по расположению модели, чтобы ref() между слоями
    (unverified_rdv/unverified_mart) всегда указывал на верную схему независимо от target.
  #}
  {%- set path = node.original_file_path or '' -%}

  {%- if path.startswith('models/unverified_rdv/') -%}
    {{ return('unverified_rdv') }}
  {%- elif path.startswith('models/unverified_mart/') -%}
    {{ return('unverified_mart') }}
  {%- elif path.startswith('models/unverified_bdv/') -%}
    {{ return('unverified_bdv') }}
  {%- elif path.startswith('models/unverified_report/') -%}
    {{ return('unverified_report') }}
  {%- elif path.startswith('models/iceberg_rzdm/') -%}
    {{ return('iceberg_rzdm') }}
  {%- else -%}
    {# Fallback: стандартная логика — custom_schema_name или target.schema #}
    {%- set default_schema = custom_schema_name if custom_schema_name is not none else target.schema -%}
    {{ return(default_schema) }}
  {%- endif -%}
{%- endmacro %}


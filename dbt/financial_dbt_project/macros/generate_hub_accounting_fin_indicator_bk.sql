{% macro generate_hub_accounting_fin_indicator_bk(prefix='s.') %}
  CONCAT_WS('|', 
    TRIM(REPLACE(COALESCE(CAST({{ prefix }}unit_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST({{ prefix }}data_type AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST({{ prefix }}metric AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST({{ prefix }}discreteness AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST({{ prefix }}value_type AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST({{ prefix }}org_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST({{ prefix }}eat_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST({{ prefix }}asset_name AS CHAR), ''), CHAR(160), '')),
    TRIM(REPLACE(COALESCE(CAST({{ prefix }}ind_code AS CHAR), ''), CHAR(160), ''))
  )
{% endmacro %}


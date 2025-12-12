{% macro generate_hub_fact_pk(hub_ind_pk, period_dt, report_date) %}
  MD5(
    CONCAT_WS('|', 
      TRIM(REPLACE(COALESCE(CAST({{ hub_ind_pk }} AS CHAR), ''), CHAR(160), '')),
      TRIM(REPLACE(COALESCE(CAST({{ period_dt }} AS CHAR), ''), CHAR(160), '')),
      TRIM(REPLACE(COALESCE(CAST({{ report_date }} AS CHAR), ''), CHAR(160), ''))
    )
  )
{% endmacro %}


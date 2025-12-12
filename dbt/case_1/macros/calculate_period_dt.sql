{% macro calculate_period_dt(year, quarter) %}
  CASE 
    WHEN {{ quarter }} = 1 THEN STR_TO_DATE(CONCAT({{ year }}, '-01-01'), '%Y-%m-%d')
    WHEN {{ quarter }} = 2 THEN STR_TO_DATE(CONCAT({{ year }}, '-04-01'), '%Y-%m-%d')
    WHEN {{ quarter }} = 3 THEN STR_TO_DATE(CONCAT({{ year }}, '-07-01'), '%Y-%m-%d')
    WHEN {{ quarter }} = 4 THEN STR_TO_DATE(CONCAT({{ year }}, '-10-01'), '%Y-%m-%d')
    ELSE STR_TO_DATE(CONCAT({{ year }}, '-01-01'), '%Y-%m-%d')
  END
{% endmacro %}


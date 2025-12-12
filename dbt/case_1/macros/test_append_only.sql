{% macro test_append_only(action='insert') %}
  {%- if action == 'insert' -%}
    {%- set sql -%}
      INSERT INTO template1_stage.stg_source_data (
        asset_name, ind_code, value_current_year, period_text, report_type,
        org_name, eat_name, unit_name, value_type, data_type, metric, discreteness,
        quarter, year, load_dttm, source_nm
      ) VALUES
      ('ТЕСТ_АКТИВ_1', 'TEST001', 1000.50, '2024-01-01', 'Бухгалтерский баланс', 
       'ТЕСТ_ОРГ_1', 'Медицинская деятельность', 'руб.', 'Фактическое', 'утвержденный', 
       'Метрика_1', 'квартал', 1, 2024, NOW(), 'raw.accounting_balance_form_1'),
      ('ТЕСТ_АКТИВ_2', 'TEST002', 2000.75, '2024-02-01', 'Бухгалтерский баланс', 
       'ТЕСТ_ОРГ_2', 'Медицинская деятельность', 'руб.', 'Фактическое', 'утвержденный', 
       'Метрика_2', 'квартал', 2, 2024, NOW(), 'raw.accounting_balance_form_1'),
      ('ТЕСТ_АКТИВ_3', 'TEST003', 3000.25, '2024-03-01', 'Бухгалтерский баланс', 
       'ТЕСТ_ОРГ_3', 'Медицинская деятельность', 'руб.', 'Фактическое', 'утвержденный', 
       'Метрика_3', 'квартал', 3, 2024, NOW(), 'raw.accounting_balance_form_1'),
      ('ТЕСТ_АКТИВ_4', 'TEST004', 4000.00, '2024-04-01', 'Бухгалтерский баланс', 
       'ТЕСТ_ОРГ_4', 'Медицинская деятельность', 'руб.', 'Фактическое', 'утвержденный', 
       'Метрика_4', 'квартал', 4, 2024, NOW(), 'raw.accounting_balance_form_1'),
      ('ТЕСТ_АКТИВ_5', 'TEST005', 5000.50, '2024-05-01', 'Бухгалтерский баланс', 
       'ТЕСТ_ОРГ_5', 'Медицинская деятельность', 'руб.', 'Фактическое', 'утвержденный', 
       'Метрика_5', 'квартал', 1, 2025, NOW(), 'raw.accounting_balance_form_1')
    {%- endset -%}
    {% do run_query(sql) %}
  {%- elif action == 'delete' -%}
    {%- set sql -%}
      DELETE FROM template1_stage.stg_source_data 
      WHERE ind_code IN ('TEST001', 'TEST002', 'TEST003', 'TEST004', 'TEST005')
        AND org_name LIKE 'ТЕСТ_ОРГ_%'
    {%- endset -%}
    {% do run_query(sql) %}
  {%- endif -%}
{% endmacro %}


{% macro union_all_source_tables() %}
{# 
  Макрос для генерации UNION ALL запросов для всех таблиц из:
    stg_asb, stg_indefinite, stg_mis, stg_asckz, stg_kuirzp, stg_nsi, stg_buinu
  исключая form_1_assets и form_2_financial
#}
  {% set tables_query %}
    SELECT 
      CONCAT('`', table_schema, '`.`', table_name, '`') as full_table_name
    FROM information_schema.tables
  WHERE table_schema IN (
    'stg_asb',
    'stg_indefinite',
    'stg_mis',
    'stg_asckz',
    'stg_kuirzp',
    'stg_nsi',
    'stg_buinu'
  )
      AND table_name NOT IN ('form_1_assets', 'form_2_financial')
      AND table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name
  {% endset %}
  
  {% set tables_result = run_query(tables_query) %}
  
  {% if execute %}
    {% set union_parts = [] %}
    {% for row in tables_result %}
      {% set table_name = row[0] %}
      {% set union_part %}
        SELECT
          `№` as file_row_number,
          `Код файла` as file_code,
          `Название файла исходное` as file_name_original,
          `Значение` as value_amt,
          `Дата` as period_dt,
          CAST(NULL AS DATETIME) as report_date,
          `Наименование показателя в исходнике` as indicator_name_source,
          `Код показателя из модели` as indicator_code_model,
          `Наименование показателя из модели` as indicator_name_model,
          `А52.Единицы измерения` as unit_name,
          `Тип данных` as data_type,
          `Метрика` as metric,
          `Дискретность` as discreteness,
          `Тип значения` as value_type,
          CAST(NULL AS VARCHAR(255)) as chuz_list,
          CAST(NULL AS VARCHAR(255)) as personnel_category,
          CAST(NULL AS VARCHAR(255)) as employee_accounting_params,
          CAST(NULL AS VARCHAR(255)) as age_groups,
          CAST(NULL AS VARCHAR(255)) as training_type,
          LOWER(CONCAT_WS('|',
            COALESCE(`А52.Единицы измерения`, ''),
            COALESCE(`Тип данных`, ''),
            COALESCE(`Метрика`, ''),
            COALESCE(`Дискретность`, ''),
            COALESCE(`Тип значения`, '')
          )) as hub_ind_bk_base,
          LOWER(CONCAT_WS('|',
            NULL,
            NULL,
            NULL,
            NULL,
            NULL
          )) as hub_ind_bk_dynamic,
          now() as load_dttm,
          '{{ table_name }}' as source_nm
        FROM {{ table_name }}
        WHERE `Код показателя из модели` IS NOT NULL
          AND `Значение` IS NOT NULL
          AND `Дата` IS NOT NULL
      {% endset %}
      {% set _ = union_parts.append(union_part) %}
    {% endfor %}
    
    {{ union_parts | join('\n\nUNION ALL\n\n') }}
  {% else %}
    -- Fallback для режима парсинга
    SELECT NULL as file_row_number LIMIT 0
  {% endif %}
{% endmacro %}


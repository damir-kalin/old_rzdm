{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['forms_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    pre_hook="SET @len_datarecord_stg_forms = (SELECT MAX(JSON_LENGTH(PARSE_JSON(`values`) -> 'datarecord')) FROM iceberg.rzdm.stg_forms);"
  )
}}

WITH numbers AS (
    SELECT generate_series AS n 
    FROM table(generate_series(0, @len_datarecord_stg_forms, 1))
)
SELECT 
    UUID() as forms_hk,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.orgname') AS VARCHAR(100)) AS orgname,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.orginn') AS VARCHAR(100)) AS orginn,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.orgkpp') AS VARCHAR(100)) AS orgkpp,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.kodform') AS VARCHAR(100)) AS kodform,
    STR_TO_DATE(CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.period1') AS VARCHAR(50)), '%d.%m.%Y') AS period1,
    STR_TO_DATE(CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.period2') AS VARCHAR(50)), '%d.%m.%Y') AS period2,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), CONCAT('$.datarecord[', n.n, '].kodstr')) AS VARCHAR(50)) AS kodstr,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), CONCAT('$.datarecord[', n.n, '].kodkol')) AS VARCHAR(10)) AS kodkol,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), CONCAT('$.datarecord[', n.n, '].summa')) AS FLOAT) AS summa,
    src.src_sys_id,
    CURRENT_TIMESTAMP as load_dttm
FROM iceberg.rzdm.stg_forms AS src
CROSS JOIN numbers AS n
WHERE src.`values` IS NOT NULL
  AND JSON_QUERY(PARSE_JSON(src.`values`), CONCAT('$.datarecord[', n.n, ']')) IS NOT NULL;
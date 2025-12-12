


WITH numbers AS (
    SELECT generate_series AS n 
    FROM table(generate_series(0, @len_datarecord_stg_kpi, 1))
) 
,
raw_data as (
SELECT 
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.orgname') AS VARCHAR(100)) AS orgname,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.orginn') AS VARCHAR(100)) AS orginn,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.orgogrn') AS VARCHAR(100)) AS orgogrn,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.kodform') AS VARCHAR(100)) AS kodform,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.nameform') AS VARCHAR(250)) AS nameform,
    STR_TO_DATE(CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.period1') AS VARCHAR(50)), '%d.%m.%Y') AS period1,
    STR_TO_DATE(CAST(JSON_QUERY(PARSE_JSON(src.`values`), '$.period2') AS VARCHAR(50)), '%d.%m.%Y') AS period2,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), CONCAT('$.datarecord[', n.n, '].nomstroki')) AS INT) AS nomstroki,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), CONCAT('$.datarecord[', n.n, '].namekol')) AS VARCHAR(250)) AS namekol,
    CAST(JSON_QUERY(PARSE_JSON(src.`values`), CONCAT('$.datarecord[', n.n, '].znach')) AS VARCHAR(300)) AS znach,
    src.src_sys_id
FROM iceberg.rzdm.stg_kpi AS src
CROSS JOIN numbers AS n
WHERE src.`values` IS NOT NULL
  AND JSON_QUERY(PARSE_JSON(src.`values`), CONCAT('$.datarecord[', n.n, ']')) IS NOT NULL
),
gp_data as (
SELECT
    orgname,
    orginn,
    orgogrn,
    kodform,
    nameform,
    period1,
    period2,
    src_sys_id,
    nomstroki,
    MAX(CASE WHEN namekol = '№ показателя' THEN znach else NULL END) AS `nom_pokazatelya`,
    MAX(CASE WHEN namekol = 'Наименование ЧУЗ' THEN znach else NULL END) AS `naimenovanie_chuz`,
    MAX(CASE WHEN namekol = 'Наименование группы показателей' THEN znach else NULL END) AS `naimenovanie_gruppy`,
    MAX(CASE WHEN namekol = 'Наименование показателя' THEN znach else NULL END) AS `naimenovanie_pokazatelya`,
    MAX(CASE WHEN namekol = 'Категория' THEN znach else NULL END) AS `kategoriya`,
    MAX(CASE WHEN namekol = 'Ед. измерения' THEN znach else NULL END) AS `edinitsa_izmereniya`,
    MAX(CASE WHEN namekol = 'План' THEN znach else NULL END) AS `plan`,
    MAX(CASE WHEN namekol = 'Факт' THEN znach else NULL END) AS `fakt`,
    MAX(CASE WHEN namekol = '% выполнения' THEN znach END) AS `protsent_vypolneniya`
FROM raw_data
GROUP BY
    orgname,
    orginn,
    orgogrn,
    kodform,
    nameform,
    period1,
    period2,
    src_sys_id,
    nomstroki
)
SELECT
    UUID() as kpi_hk,
    orgname,
    orginn,
    orgogrn,
    kodform,
    nameform,
    period1,
    period2,
    src_sys_id,
    nomstroki,
    nom_pokazatelya,
    naimenovanie_chuz,
    naimenovanie_gruppy,
    naimenovanie_pokazatelya,
    kategoriya,
    edinitsa_izmereniya,
    plan,
    fakt,
    protsent_vypolneniya,
    CURRENT_TIMESTAMP as load_dttm
FROM gp_data;
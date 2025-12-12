{{
  config(
    materialized='table',
    table_type='PRIMARY',
    keys=['contractor_debt_hk'],
    buckets=3,
    properties={
      'replication_num': '1'
    },
    pre_hook="SET @len_items_stg_contractor_debt = (select max(json_length(parse_json(`values`) -> 'items')) from iceberg.rzdm.stg_contractor_debt);"
  )
}}


WITH numbers AS (
    SELECT generate_series as n FROM table(generate_series(0, @len_items_stg_contractor_debt, 1)) 
)
SELECT 
	UUID() as contractor_debt_hk,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].Organization')) AS VARCHAR(65333)) AS organization,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].OrganizationINN')) AS VARCHAR(50)) AS organization_inn,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].OrganizationKPP')) AS VARCHAR(50)) AS organization_kpp,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].OrganizationMDMKey')) AS VARCHAR(100)) AS organization_mdm_key,
    
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].Counterparty')) AS VARCHAR(250)) AS counterparty,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].CounterpartyINN')) AS VARCHAR(50)) AS counterparty_inn,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].CounterpartyKPP')) AS VARCHAR(50)) AS counterparty_kpp,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].CounterpartyMDMKey')) AS VARCHAR(100)) AS counterparty_mdm_key,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].Contract')) AS VARCHAR(150)) AS contract,
    CAST(REPLACE(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].AmountOwed')), ' ', '') as INT) AS amount_owed,
    STR_TO_DATE(CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].TimeStamp')) AS VARCHAR(50)), '%d.%m.%Y %H:%i:%s')  AS timestamp,
    src.src_sys_id,
    CURRENT_TIMESTAMP as load_dttm
FROM iceberg.rzdm.stg_contractor_debt AS src
CROSS JOIN numbers AS n
WHERE src.`values` IS NOT NULL
  AND json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, ']')) IS NOT NULL;
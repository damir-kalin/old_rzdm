

  create table `unverified_rdv`.`hub_rdv__employees`
      PRIMARY KEY (employees_hk)
    PROPERTIES (
      "replication_num" = "1"
    )
  as 


WITH numbers AS (
    SELECT generate_series as n FROM table(generate_series(0, @len_items_stg_employees, 1)) 
)
SELECT 
	UUID() as employees_hk,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].Organization')) AS VARCHAR(65333)) AS organization,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].OrganizationINN')) AS VARCHAR(50)) AS organization_inn,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].OrganizationKPP')) AS VARCHAR(50)) AS organization_kpp,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].OrganizationMDMKey')) AS VARCHAR(100)) AS organization_mdm_key,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].Division')) AS VARCHAR(65333)) AS division,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].DivisionMDMKey')) AS VARCHAR(100)) AS division_mdm_key,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].Employee')) AS VARCHAR(100)) AS employee,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].TableNumber')) AS VARCHAR(50)) AS table_number,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].Post')) AS VARCHAR(65333)) AS post,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].Gender')) AS VARCHAR(20)) AS gender,
    CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].TypeOfEmployment')) AS VARCHAR(50)) AS type_of_employment,
    STR_TO_DATE(CAST(json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, '].TimeStamp')) AS VARCHAR(50)), '%d.%m.%Y %H:%i:%s')  AS timestamp,
    src.src_sys_id,
    CURRENT_TIMESTAMP as load_dttm
FROM iceberg.rzdm.stg_employees AS src
CROSS JOIN numbers AS n
WHERE src.`values` IS NOT NULL
  AND json_query(parse_json(src.`values`), CONCAT('$.items[', n.n, ']')) IS NOT NULL;
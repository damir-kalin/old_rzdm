-- Создание внешнего каталога iceberg
CREATE EXTERNAL CATALOG IF NOT EXISTS 'iceberg'
COMMENT "External catalog to Apache Iceberg on MinIO"
PROPERTIES
(
    "type"="iceberg",
    "iceberg.catalog.type"="rest",
    "iceberg.catalog.uri"="http://iceberg-rest:8181",
    "iceberg.catalog.warehouse"="${SR_BUCKET_ICEBERG_NAME}",
    "aws.s3.access_key"="${MINIO_ROOT_USER}",
    "aws.s3.secret_key"="${MINIO_ROOT_PASSWORD}",
    "aws.s3.endpoint"="http://minio:9000",
    "aws.s3.enable_path_style_access"="true",
    "client.factory"="com.starrocks.connector.iceberg.IcebergAwsClientFactory"
);

-- Создание внешнего ресурса jdbc_service_layer для доступа к сервисной базе данных
CREATE EXTERNAL RESOURCE jdbc_service_layer
    PROPERTIES (
        "type"="jdbc",
        "user"="airflow",
        "password"="Q1w2e3r+",
        "jdbc_uri"="jdbc:postgresql://postgres-airflow:5432/service_db",
        "driver_url"="file:////opt/starrocks/postgresql-42.6.0.jar",
        "driver_class"="org.postgresql.Driver"
);

-- Создание базовых баз данных
CREATE DATABASE IF NOT EXISTS stage;
CREATE DATABASE IF NOT EXISTS main_rdv;
CREATE DATABASE IF NOT EXISTS main_bdv;
CREATE DATABASE IF NOT EXISTS main_mart;
CREATE DATABASE IF NOT EXISTS main_report;

-- Создание sandbox баз данных
CREATE DATABASE IF NOT EXISTS sandbox_db_commercial;
CREATE DATABASE IF NOT EXISTS sandbox_db_medical;
CREATE DATABASE IF NOT EXISTS sandbox_db_hr;
CREATE DATABASE IF NOT EXISTS sandbox_db_accounting;    
CREATE DATABASE IF NOT EXISTS sandbox_db_financial;

-- Создание пользователей
CREATE USER IF NOT EXISTS 'commercial_user' IDENTIFIED BY 'Q1w2e3r+';
CREATE USER IF NOT EXISTS 'medical_user' IDENTIFIED BY 'Q1w2e3r+';
CREATE USER IF NOT EXISTS 'hr_user' IDENTIFIED BY 'Q1w2e3r+';
CREATE USER IF NOT EXISTS 'accounting_user' IDENTIFIED BY 'Q1w2e3r+';
CREATE USER IF NOT EXISTS 'financial_user' IDENTIFIED BY 'Q1w2e3r+';

-- Установка паролей
SET PASSWORD FOR 'commercial_user' = PASSWORD('Q1w2e3r+');
SET PASSWORD FOR 'medical_user' = PASSWORD('Q1w2e3r+');
SET PASSWORD FOR 'hr_user' = PASSWORD('Q1w2e3r+');
SET PASSWORD FOR 'accounting_user' = PASSWORD('Q1w2e3r+');
SET PASSWORD FOR 'financial_user' = PASSWORD('Q1w2e3r+');

-- Отзыв всех привилегий на все базы данных (для изоляции)
REVOKE ALL PRIVILEGES ON *.* FROM 'commercial_user';
REVOKE ALL PRIVILEGES ON *.* FROM 'medical_user';
REVOKE ALL PRIVILEGES ON *.* FROM 'hr_user';
REVOKE ALL PRIVILEGES ON *.* FROM 'accounting_user';
REVOKE ALL PRIVILEGES ON *.* FROM 'financial_user';

-- Настройка привилегий для commercial_user
GRANT SELECT ON ALL VIEWS IN DATABASE sandbox_db_commercial TO 'commercial_user';

-- Настройка привилегий для medical_user
GRANT SELECT ON ALL VIEWS IN DATABASE sandbox_db_medical TO 'medical_user';

-- Настройка привилегий для hr_user
GRANT SELECT ON ALL VIEWS IN DATABASE sandbox_db_hr TO 'hr_user';

-- Настройка привилегий для accounting_user
GRANT SELECT ON ALL VIEWS IN DATABASE sandbox_db_accounting TO 'accounting_user';

-- Настройка привилегий для financial_user
GRANT SELECT ON ALL VIEWS IN DATABASE sandbox_db_financial TO 'financial_user';

-- Создание базы данных rzdm в iceberg каталоге
SET CATALOG iceberg;
CREATE DATABASE IF NOT EXISTS rzdm;
USE rzdm;

SET CATALOG iceberg;

USE rzdm;

-- Создание тестовой таблицы в iceberg.rzdm базе данных

CREATE TABLE IF NOT EXISTS iceberg.rzdm.test (
    a int DEFAULT NULL COMMENT "A",
    b double DEFAULT NULL COMMENT "B",
    c varchar DEFAULT NULL COMMENT "C",
    dttm datetime DEFAULT NULL COMMENT "DTTM"
);


CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_contractor_debt (
    `values` VARCHAR COMMENT 'JSON данные (не распарсенные)',
    src_sys_id VARCHAR(10) DEFAULT 'asb' COMMENT 'Источник данных',
    load_dttm DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Время загрузки'
);

CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_contractors (
    `values` VARCHAR COMMENT 'JSON данные (не распарсенные)',
    src_sys_id VARCHAR(10) DEFAULT 'nsi' COMMENT 'Источник данных',
    load_dttm DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Время загрузки'
);

CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_employees (
    `values` VARCHAR COMMENT 'JSON данные (не распарсенные)',
    src_sys_id VARCHAR(10) DEFAULT 'kuirzp' COMMENT 'Источник данных',
    load_dttm DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Время загрузки'
);

CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_forms (
    `values` VARCHAR COMMENT 'JSON данные (не распарсенные)',
    src_sys_id VARCHAR(10) DEFAULT 'buinu' COMMENT 'Источник данных',
    load_dttm DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Время загрузки'
);

CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_kpi (
    `values` VARCHAR COMMENT 'JSON данные (не распарсенные)',
    src_sys_id VARCHAR(10) DEFAULT 'kuirzp' COMMENT 'Источник данных',
    load_dttm DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Время загрузки'
);

CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_organizations (
    `values` VARCHAR COMMENT 'JSON данные (не распарсенные)',
    src_sys_id VARCHAR(10) DEFAULT 'nsi' COMMENT 'Источник данных',
    load_dttm DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Время загрузки'
);

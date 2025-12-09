-- Create stg_buinu database if not exists
CREATE DATABASE IF NOT EXISTS stg_buinu;

-- Form 1 Assets Table
CREATE TABLE IF NOT EXISTS stg_buinu.form_1_assets (
    `АКТИВ` varchar(65333) NULL COMMENT 'Значение из одноименного поля исходного файла формы 1, пробелы в начале и в конце обрезаем',
    `Код показателя` varchar(65333) NULL COMMENT 'Значение из одноименного поля исходного файла формы 1',
    `На текущий год` decimal(38, 10) NULL COMMENT 'Значение из одноименного поля исходного файла формы 1',
    `На 31 декабря прошлого года` decimal(38, 10) NULL COMMENT 'Значение из одноименного поля исходного файла формы 1',
    `На 31 декабря позапрошлого года` decimal(38, 10) NULL COMMENT 'Значение из одноименного поля исходного файла формы 1',
    `Отчетный период` varchar(65333) NULL COMMENT 'Отчетный период из шапки документа',
    `Единица измерения` varchar(65333) NULL COMMENT 'Единица измерения из шапки документа',
    `Название отчета` varchar(65333) NULL COMMENT 'Название отчета из шапки документа',
    `Организация` varchar(65333) NULL COMMENT 'Имя организации из шапки документа',
    `Вид экономической деятельности` varchar(65333) NULL COMMENT 'Вид экономической деятельности из шапки документа',
    `file_id` varchar(300) NULL COMMENT 'Идентификатор файла, взятый из имени файла',
    `user_name` varchar(300) NULL COMMENT 'Имя пользователя взятое из имени файла',
    `raw_file_name` varchar(2000) NULL COMMENT 'Имя файла целиком',
    `file_name` varchar(65333) NULL COMMENT 'Имя файла не включающее в себя служебные данные',
    `s3_placement` datetime NULL COMMENT 'Дата и время, когда файл оказался в бакете'
) ENGINE=OLAP
DUPLICATE KEY(`АКТИВ`, `Код показателя`)
DISTRIBUTED BY HASH(`file_id`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT"
);

-- Form 2 Financial Results Table
CREATE TABLE IF NOT EXISTS stg_buinu.form_2_financial (
    `Показатель` varchar(65333) NULL COMMENT 'Значение из одноименного поля исходного файла формы 2',
    `Код` varchar(65333) NULL COMMENT 'Значение из одноименного поля исходного файла формы 2',
    `За текущий период` decimal(38, 10) NULL COMMENT 'Значение из одноименного поля исходного файла формы 2',
    `За текущий период прошлого года` decimal(38, 10) NULL COMMENT 'Значение из одноименного поля исходного файла формы 2',
    `Отчетный период` varchar(65333) NULL COMMENT 'Отчетный период из шапки документа',
    `Единица измерения` varchar(65333) NULL COMMENT 'Единица измерения из шапки документа',
    `Название отчета` varchar(65333) NULL COMMENT 'Название отчета из шапки документа',
    `Организация` varchar(65333) NULL COMMENT 'Имя организации из шапки документа',
    `file_id` varchar(300) NULL COMMENT 'Идентификатор файла, взятый из имени файла',
    `user_name` varchar(300) NULL COMMENT 'Имя пользователя взятое из имени файла',
    `raw_file_name` varchar(2000) NULL COMMENT 'Имя файла целиком',
    `file_name` varchar(65333) NULL COMMENT 'Имя файла не включающее в себя служебные данные',
    `s3_placement` datetime NULL COMMENT 'Дата и время, когда файл оказался в бакете'
) ENGINE=OLAP
DUPLICATE KEY(`Показатель`, `Код`)
DISTRIBUTED BY HASH(`file_id`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "DEFAULT"
);

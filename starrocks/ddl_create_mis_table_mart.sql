-- ============================================================================
-- DDL для таблиц МИС
-- ============================================================================
SET CATALOG default_catalog;


USE unverified_mart;

-- ============================================================================
-- 1. H_SCHEDULE Стационар. История пребывания пациентов (интервалы лежания) по местам, палатам и отделениям
-- ============================================================================
drop TABLE if exists unverified_mart.medical__dm_H_SCHEDULE_hstat;
CREATE TABLE medical__dm_H_SCHEDULE_hstat (
    HSID  BIGINT NOT NULL COMMENT 'Идентификатор интервала лежания. Генератор [HSID_GEN]',
    load_dttm DATETIME  DEFAULT CURRENT_TIMESTAMP,    
    
    DIRID BIGINT NULL COMMENT 'Идентификатор стационарного лечения (лежания) [STAT_DIRECTION.SDID]',    
    NROW  INT NULL COMMENT 'Номер  интервала лежания: порядковый номер интервала в рамках данного лежания(начиная  с 1)0, если  данный интервал является резервом на время перевода',    
    RESERVEID BIGINT NULL COMMENT 'Идентификатор  резерва, связанного с данным интервалом лежания [H_SCHEDULE.HSID]',    
    WORKDATE DATETIME NULL COMMENT 'Дата и  время начала интервала лежания',    
    PLACEID BIGINT NULL COMMENT 'Идентификатор  места в палате (койки) [CHAIRS.CHID]',    
    ENDDATE DATETIME NULL COMMENT 'Дата и  время окончания интервала лежания',    
    PCODE BIGINT NULL COMMENT 'Идентификатор  пациента [CLIENTS.PCODE]',   
    FILIAL INT NULL COMMENT 'Идентификатор  филиала [FILIALS.FILID]',   
    RESERVNUM INT NULL COMMENT 'НЕ  ИСПОЛЬЗУЕТСЯ (Стационар)',   
    UID BIGINT NULL COMMENT 'Идентификатор  последнего изменившего интервал лежания [DOCTOR.DCODE]',   
    MODIFYDATE DATETIME NULL COMMENT 'Дата и  время последнего изменения интервала лежания',   
    EXPORTDATE DATETIME NULL COMMENT 'НЕ  ИСПОЛЬЗУЕТСЯ (Стационар)', 
    `COMMENT` VARCHAR(256) NULL COMMENT 'Комментарий',  
    ISCLOSED SMALLINT NULL COMMENT 'НЕ  ИСПОЛЬЗУЕТСЯ (Стационар)',   
    ROOMID BIGINT NULL COMMENT 'Идентификатор  палаты [ROOMS.RID]',  
    STATDEP BIGINT NULL COMMENT 'Идентификатор  стационарного отделения [STAT_DEPARTMENTS.SDEPID]',   
    DIAG BIGINT NULL COMMENT 'Идентификатор  диагноза перевода [DIAGNOSIS.DGCODE] НЕ ИСПОЛЬЗУЕТСЯ c 15.1 (Стационар)',   
    RTYPEID BIGINT NULL COMMENT 'Тип  палаты для данного интервала лежания:[ROOMTYPES.RTYPEID], если для данного интервала тип определен0 или  null, если тип для данного интервала не определен и берется из настроек  текущей палаты'
    ,KSG_SCHID BIGINT NULL COMMENT 'Идентификатор  услуги КСГ [WSCHEMA.SCHID] НЕ ИСПОЛЬЗУЕТСЯ c 13.1 (Стационар)'    
    ,SAL_DEPID BIGINT NULL COMMENT 'Идентификатор  структурного подразделения организации [SAL_DEPARTMENTS.OBJID]'   
    ,REPL$ID  INT NULL COMMENT 'Идентификатор  репликации'   
    ,REPL$GRPID  INT NULL COMMENT 'Идентификатор  группы репликации'    
    , load_id BIGINT NULL
    , valid_from_dttm DATETIME NULL DEFAULT '1980-12-31 00:00'
    , valid_to_dttm DATETIME NULL DEFAULT '9999-12-31 23:59'
    , src_sys_id VARCHAR(10) NULL DEFAULT 'mis'
    , op VARCHAR(10) NULL
    , ts_ms BIGINT NULL
    , source_ts_ms BIGINT NULL 
	, deleted_flg varchar(1) NULL DEFAULT 'N'   
    , extract_dttm DATETIME as  cast(from_unixtime(ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)   
    , src_chng_dttm DATETIME  as  cast(from_unixtime(source_ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)       	
) ENGINE=OLAP 
PRIMARY KEY(HSID, load_dttm) 
DISTRIBUTED BY hash(HSID) buckets 1
order by (HSID, load_dttm) 
PROPERTIES (
"bucket_size" = "4294967296",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"fast_schema_evolution" = "true"
);


-- ============================================================================
-- 2. CHAIRS Структура клиники. Справочник рабочих мест.
-- ============================================================================
drop TABLE if exists unverified_mart.medical__dm_CHAIRS_dim;
CREATE TABLE medical__dm_CHAIRS_dim (
CHID	BIGINT NOT NULL COMMENT 'Идентификатор.  Генератор [SPRAV_GEN]'
, load_dttm DATETIME  DEFAULT CURRENT_TIMESTAMP 

,CHNAME	VARCHAR(150)	NULL COMMENT 'Название  рабочего места.'
,CHCOLOR	INTEGER	NULL COMMENT 'Цвет  рабочего места'
,RID	BIGINT	NULL COMMENT 'Идентификатор  кабинета (палаты). [ROOMS.RID]'
,FILIAL	INTEGER	NULL COMMENT 'Код  филиала. [FILIALS.FILID]'
,SCHEDTYPE	INTEGER	NULL COMMENT '1 -  обычное рабочее место 2 - рабочее место операционного расписания'
,CHID1	BIGINT NULL COMMENT 'Идентификатор  связанного рабочего места. [CHAIRS.CHID]'
,ISCOMMON	INTEGER	NULL COMMENT '1 -  Общее рабочее место'
,DOCTREQ	INTEGER NULL COMMENT '1 -  Доктор обязателен при заполнении для общего рабочего места.'
,DISDATE	DATETIME NULL COMMENT 'Не  действительно с указанной даты.'
,DEPNUM	BIGINT NULL COMMENT 'Идентификатор  отделения. [DEPARTMENTS.DEPNUM]'
,GROUPTREAT	INTEGER	NULL COMMENT 'ВОЗМОЖНО  НЕ ИСПОЛЬЗУЕТСЯ!!!'
,DOCTSELTYPE	INTEGER	NULL COMMENT '1 -  Выбор доктора при назначении в общее рабочее место по правам доступа.'
,STAT_CHAIR_TYPE	BIGINT	NULL COMMENT 'Тип  койки для стационарного места из справочника [STAT_CHAIRS_TYPE.]'
,MODALITY	VARCHAR(64)	NULL COMMENT 'Модальность'
,TREATTIMEREG	SMALLINT	NULL COMMENT '1 -  Фиксировать время приема'
,SCHEDINFO	VARCHAR(2048) NULL COMMENT 'Текст в  расписании'
,TMSTATUS	BIGINT	NULL COMMENT 'Вид  автоматического резерва [SHEDMARKS.MRKID]'
,WLPID	BIGINT	NULL COMMENT ' Целевая  система WL [PHPROGRREF.PID]'
,PACSPID	BIGINT	NULL COMMENT ' Целевая  система PACS [PHPROGRREF.PID]'
,DAYHOSPITAL	SMALLINT NULL COMMENT '1 -  дневной стационар'
,DOCTWTIMECHECK	SMALLINT NULL COMMENT 'Выбор  врача в назначении:0 - В выпадающем списке, по графику1 - В отдельной форме, по графику2 - В отдельной форме, без привязки к графику3 - По общей настройке'
,FCFSWORKTYPE	SMALLINT NULL COMMENT 'Работа с  живой очередью: 0 - не используется1 - может использоваться2 - используется по умолчанию'
,SCHEDGRIDTYPE	SMALLINT NULL COMMENT 'Тип  сетки расписания:0 - Стандартная;1 - Фиксированная;'
,REPL$ID	INTEGER	NULL COMMENT ' Идентификатор  репликации'
,REPL$GRPID	INTEGER	NULL COMMENT ' Идентификатор  группы репликации'
,NAPRAVREQ	SMALLINT NULL COMMENT '1 -  обязательно указание направления'
,VIEWINWEB	SMALLINT NULL COMMENT '1 -  Отображать на сайте'
,WEBNAME VARCHAR(128) NULL COMMENT 'Название  для сайта'
,SEXCONTROLTYPE	INTEGER	NULL COMMENT '1 - Отключить  проверку пола пациента при госпитализации'
,FILIALTREATTYPE INTEGER NULL COMMENT '1 -  Возможность создания приёма по назначению другого филиала.'
,ISTMC	INTEGER	NULL COMMENT ''
,PLACEID	BIGINT	NULL COMMENT 'Тип  протокола ИБ [WORKPLACEDOC.PLACEID].'
,PACSCODE	VARCHAR(24) NULL COMMENT 'Код РМ  для PACS'
,SHINTERV	INTEGER NULL COMMENT 'Стандартный  интервал приёма (мин)'
,NUMDAYSVIS	INTEGER	NULL COMMENT 'Отображать  расписание на сайте и в инфомате на .. дней вперед'
,DEFSCHDCODE	BIGINT	NULL COMMENT ' Доктор  по умолчанию (планируемые услуги)'
,MRKID	BIGINT NULL COMMENT '	Тип  резерва [SHEDMARKS.MRKID] - Автоматически устанавливать указанный резерв  после установленной записи в расписании РМ'
,RESERVEDURATION 	INTEGER	NULL COMMENT 'Длительность  резерва (минут) после постановки записи в расписании РМ'
,TALONTEMPLATE	VARCHAR(100)	NULL COMMENT ''
,PRINTTALON_CLVISIT	SMALLINT	NULL COMMENT ''
    , load_id BIGINT NULL
    , valid_from_dttm DATETIME NULL DEFAULT '1980-12-31 00:00'
    , valid_to_dttm DATETIME NULL DEFAULT '9999-12-31 23:59'   
    -- , extract_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    -- , src_chng_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    , src_sys_id VARCHAR(10) NULL DEFAULT 'mis'
    , op VARCHAR(10) NULL
    , ts_ms BIGINT NULL
    , source_ts_ms BIGINT NULL 
	, deleted_flg varchar(1) NULL DEFAULT 'N'   
    , extract_dttm DATETIME as  cast(from_unixtime(ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)   
    , src_chng_dttm DATETIME  as  cast(from_unixtime(source_ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)       		
) ENGINE=OLAP 
PRIMARY KEY(CHID, load_dttm) 
DISTRIBUTED BY hash(CHID) buckets 1
order by (CHID, load_dttm) 
PROPERTIES (
"bucket_size" = "4294967296",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"fast_schema_evolution" = "true"
);


-- ============================================================================
-- 3. ROOMS Структура клиники. Справочник кабинетов (палат).
-- ============================================================================
drop TABLE if exists unverified_mart.medical__dm_ROOMS_dim;
 CREATE TABLE medical__dm_ROOMS_dim (
 RID BIGINT NOT NULL COMMENT 'Идентификатор  кабинета. Генератор [SPRAV_GEN]'
, load_dttm DATETIME  DEFAULT CURRENT_TIMESTAMP  
,RNUM	VARCHAR(25)	NULL COMMENT 'Номер  кабинета.'
,RNAME	VARCHAR(256)	NULL COMMENT 'Название  кабинета.'
,DEPNUM	BIGINT	NULL COMMENT 'Отделение  кабинета. [DEPARTMENTS.DEPNUM]'
,FILIAL	INTEGER NULL COMMENT 'Филиал.[FILIALS.FILID]'
,WARE	SMALLINT	NULL COMMENT 'Определяется  по [ROOMS.MAINTYPE].Если  [ROOMS.MAINTYPE] = 2, то 1, иначе 0Регулирует видимость кабинета внутри  складского модуля'
,RTYPE	BIGINT	NULL COMMENT 'Подтип  кабинета [DICINFO.DICID {REFID = 26}]'
,CASHID	BIGINT	NULL COMMENT 'Подразделение  кабинета. [CASHREF.CASHID]'
,RAZDEL	BIGINT	NULL COMMENT 'Раздел  кабинета. [RAZDEL.RAZDID]'
,KATEG	INTEGER	NULL COMMENT 'ВОЗМОЖНО  НЕ ИСПОЛЬЗУЕТСЯ!!!'
,PLACECOUNT	INTEGER	NULL COMMENT 'ВОЗМОЖНО  НЕ ИСПОЛЬЗУЕТСЯ!!!'
,INVENTCODE	INTEGER	NULL COMMENT 'Склад.  Код ивнторизации'
,INVENTDATE	DATETIME	NULL COMMENT 'Склад.  Дата инвентаризации'
,ISTRADE	SMALLINT	NULL COMMENT 'Склад.  (Уточнить у Володи)'
,IDBUILD	BIGINT	NULL COMMENT 'Код  корпуса. [BUILDINGS.IDBUILD]'
,IDFLOOR	BIGINT	NULL COMMENT 'Код  этажа. [FLOORS.IDFLOOR]'
,MAINTYPE	BIGINT	NULL COMMENT 'Вид  кабинета [DICINFO.DICID {REFID = 63}]. Основной тип (кабинет, склад, палата).'
,NAZNCOUNT	INTEGER	NULL COMMENT 'Количество  одновременных назначений.'
,GROUPTREAT	INTEGER	NULL COMMENT '1 -  Использовать групповые назначения.'
,CLIENTCOUNT	INTEGER	NULL COMMENT 'Максимальное  количество пациентов при групповом назначении.'
,CLIENTCHECK_DIS	INTEGER	NULL COMMENT '1 - Нет  запрета на одновременные назначения в кресла.'
,NAZNCOPYMODE	INTEGER	NULL COMMENT '1 -  Копирование: Использовать стандартный интервал.'
,JID	BIGINT	NULL COMMENT 'Юр.  лицо. [JPERSONS.JID]'
,PHONE	VARCHAR(48)	NULL COMMENT 'Телефон  кабинета.'
,COMMENT	VARCHAR(255) NULL COMMENT 'Комментарий  к кабинету.'
,INTENSIVE	SMALLINT NULL COMMENT '1 -  Палата интенсивной терапии.'
,DISDATE	DATETIME NULL COMMENT 'Не  действует с даты'
,COSTTYPE	INTEGER	NULL COMMENT 'Склад.  Расчет цены 0 - средняя 1 - ФИФО'
,PARTTYPE	INTEGER	NULL COMMENT 'Не  используется'
,CENTRAL	SMALLINT	NULL COMMENT '1 -  Центральный склад'
,MATTYPE	INTEGER	NULL COMMENT '1 -  Место хранения материалов'
,FINSOURCE	VARCHAR(512) NULL COMMENT 'Список  источников финансирования [FINSOURCE.FINID]'
,AUTOUNPACK	SMALLINT NULL COMMENT 'Автоматическая  разукомплектация при выдаче в кабинет:0 - не используется1 - без группировки2 - с группировкой'
,REPL$ID	INTEGER	NULL COMMENT 'Идентификатор  репликации'
,REPL$GRPID	INTEGER NULL COMMENT 'Идентификатор  группы репликации'
,ROOMVISITTYPE	BIGINT NULL COMMENT 'Тип  приёма в кабинете [DICINFO.DICID {REFID = -1000}]'
,WEBNUM	VARCHAR(64)	NULL COMMENT 'Номер  кабинета для сайта'
,STAFFDEPID	BIGINT	NULL COMMENT 'Подразделение  по штатному расписанию [DICINFO.DICID {REFID = -101}]'
,MMPID	VARCHAR(48)	NULL COMMENT 'Идентификатор  места хранения в системе МДЛП'
,MMPJID	BIGINT	NULL COMMENT 'Идентификатор  юридического лица для МДЛП [JPERSONS.JID]'
,RVURL	VARCHAR(128) NULL COMMENT 'URL-адрес  для доступа к регистратору выбытия (РВ)'
,RVID	VARCHAR(48) NULL COMMENT 'Идентификатор  РВ'
,RVLOGIN	VARCHAR(128) NULL COMMENT 'Логин  РВ'
,RVPASSWORD	VARCHAR(128) NULL COMMENT 'Пароль  РВ'
,STORAGEPLACE	INTEGER	NULL COMMENT '1 -  является местом ответственного хранения'
,SHORTNAME	VARCHAR(50)	NULL COMMENT 'Краткое  название кабинета'
,FINANALYSIS	INTEGER	 NULL COMMENT '1 -  Формирование проводок для финансового анализа'
,CONTACTNAME	VARCHAR(255) NULL COMMENT 'Контактное  лицо, ФИО контактного лица домового хозяйства'
,ISDUTYROOM	INTEGER	NULL COMMENT ''
,GROUPSERVICETYPE	SMALLINT	NULL COMMENT ''
,FRMOID	VARCHAR(40)	 NULL COMMENT ''
,NOTFROMDB VARCHAR(50)	 NULL COMMENT ''
    , load_id BIGINT NULL
    , valid_from_dttm DATETIME NULL DEFAULT '1980-12-31 00:00'
    , valid_to_dttm DATETIME NULL DEFAULT '9999-12-31 23:59'    
    -- , extract_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    -- , src_chng_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    , src_sys_id VARCHAR(10) NULL DEFAULT 'mis'
    , op VARCHAR(10) NULL
    , ts_ms BIGINT NULL
    , source_ts_ms BIGINT NULL 
	, deleted_flg varchar(1) NULL DEFAULT 'N'   
    , extract_dttm DATETIME as  cast(from_unixtime(ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)   
    , src_chng_dttm DATETIME  as  cast(from_unixtime(source_ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)       		
) ENGINE=OLAP 
PRIMARY KEY(RID, load_dttm) 
DISTRIBUTED BY hash(RID) buckets 1
order by (RID, load_dttm) 
PROPERTIES (
"bucket_size" = "4294967296",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"fast_schema_evolution" = "true"
);

-- ============================================================================
-- 4. ROOMGRMAIN Структура клиники. Состав группы палат
-- ============================================================================
drop TABLE if exists unverified_mart.medical__dm_ROOMGRMAIN_dim;
 CREATE TABLE medical__dm_ROOMGRMAIN_dim(
 GRRID BIGINT NOT NULL COMMENT 'Идентификатор  группы палат. Генератор [SPRAV_GEN]'
    , load_dttm DATETIME  DEFAULT CURRENT_TIMESTAMP  
,GROUPID	BIGINT	NULL COMMENT 'Код  группы. [ROOMGROUP.GROUPID]'
,RID	BIGINT	NULL COMMENT 'Код  палаты. [ROOMS.RID]'
,FILIAL	INTEGER	NULL COMMENT 'Код  филиала. [FILIALS.FILID]'
,IDBUILD	BIGINT	NULL COMMENT 'Код  корпуса. [BUILDINGS.IDBUILD]'
,IDFLOOR	BIGINT	NULL COMMENT 'Код  этажа. [FLOORS.IDFLOOR]'
,GTYPE	INTEGER	NULL COMMENT 'Тип  вхождения: 1 - элемент является корпусом, 2 - этажом , 3 - палатой.'
,REPL$ID	INTEGER	NULL COMMENT 'Идентификатор  репликации'
,REPL$GRPID	INTEGER	NULL COMMENT 'Идентификатор  группы репликации'
    , load_id BIGINT NULL
    , valid_from_dttm DATETIME NULL DEFAULT '1980-12-31 00:00'
    , valid_to_dttm DATETIME NULL DEFAULT '9999-12-31 23:59'  
    -- , extract_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    -- , src_chng_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    , src_sys_id VARCHAR(10) NULL DEFAULT 'mis'
    , op VARCHAR(10) NULL
    , ts_ms BIGINT NULL
    , source_ts_ms BIGINT NULL 
	, deleted_flg varchar(1) NULL DEFAULT 'N'   
    , extract_dttm DATETIME as  cast(from_unixtime(ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)   
    , src_chng_dttm DATETIME  as  cast(from_unixtime(source_ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)       		
	
) ENGINE=OLAP 
PRIMARY KEY(GRRID, load_dttm) 
DISTRIBUTED BY hash(GRRID) buckets 1
order by (GRRID, load_dttm) 
PROPERTIES (
"bucket_size" = "4294967296",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"fast_schema_evolution" = "true"
);


-- ============================================================================
-- 5. ROOMGROUP Структура клиники. Группы палат
-- ============================================================================

drop TABLE if exists unverified_mart.medical__dm_ROOMGROUP_dim;
 CREATE TABLE medical__dm_ROOMGROUP_dim(
 GROUPID BIGINT NOT NULL COMMENT 'Идентификатор  группы палат. Генератор [SPRAV_GEN]'
    , load_dttm DATETIME  DEFAULT CURRENT_TIMESTAMP 
,GRTYPE	SMALLINT	NULL COMMENT 'Тип  группы: 1 - без пересечений. 2 - с пересечениями.'
,GRNAME	VARCHAR(255)	NULL COMMENT 'Название  группы.'
,FILIAL	INTEGER	NULL COMMENT 'Филиал.  [FILIALS.FILID]'
,IDBUILD	BIGINT	NULL COMMENT 'Код  корпуса. [BUILDINGS.IDBUILD]'
,IDFLOOR	BIGINT	NULL COMMENT 'Код  этажа. [FLOORS.IDFLOOR]'
,USEINPRIEM	SMALLINT	NULL COMMENT '1 - Использовать в приемной.'
,STATDEP	BIGINT	NULL COMMENT 'Код  стационарного отделения, в которое входит группа. [STAT_DEPARTMENTS.SDEPID]'
,REPL$ID	INTEGER	NULL COMMENT 'Идентификатор  репликации'
,REPL$GRPID	INTEGER	NULL COMMENT 'Идентификатор  группы репликации'
    , load_id BIGINT NULL
    , valid_from_dttm DATETIME NULL DEFAULT '1980-12-31 00:00'
    , valid_to_dttm DATETIME NULL DEFAULT '9999-12-31 23:59'   
    -- , extract_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    -- , src_chng_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    , src_sys_id VARCHAR(10) NULL DEFAULT 'mis'
    , op VARCHAR(10) NULL
    , ts_ms BIGINT NULL
    , source_ts_ms BIGINT NULL 
	, deleted_flg varchar(1) NULL DEFAULT 'N'   
    , extract_dttm DATETIME as  cast(from_unixtime(ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)   
    , src_chng_dttm DATETIME  as  cast(from_unixtime(source_ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)       		
) ENGINE=OLAP 
PRIMARY KEY(GROUPID, load_dttm) 
DISTRIBUTED BY hash(GROUPID) buckets 1
order by (GROUPID, load_dttm) 
PROPERTIES (
"bucket_size" = "4294967296",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"fast_schema_evolution" = "true"
);


-- ============================================================================
-- 6. MIS_STAT_DEPARTMENTS Стационар. Стационарные отделения
-- ============================================================================

drop TABLE if exists unverified_mart.medical__dm_STAT_DEPARTMENTS_dim;
 CREATE TABLE medical__dm_STAT_DEPARTMENTS_dim(
SDEPID	BIGINT NOT NULL COMMENT 'Идентификатор  стационарного отделения. Генератор [PROFOSMOTR_GEN]'
    , load_dttm DATETIME  DEFAULT CURRENT_TIMESTAMP 
,SDEPNAME	VARCHAR(255) NULL COMMENT '	Название  стационарного отделения'
,FILIAL	INTEGER	NULL COMMENT 'Идентификатор  филиала, к которому относится стационарное отделение [FILIALS.FILID]'
,DEPCOLOR	INTEGER	NULL COMMENT 'Цвет,  которым будет отображаться стационарное отделение в истории переводов'
,DEPID	BIGINT	NULL COMMENT 'Идентификатор  медицинского профиля по умолчанию [DEPARTMENTS.DEPNUM]'
,CASHID	BIGINT	NULL COMMENT 'Идентификатор  кассы (подразделения) по умолчанию [CASHREF.CASHID]'
,DEPCODE	VARCHAR(128)	NULL COMMENT 'Текстовый  код стационарного отделения'
,RID	BIGINT	NULL COMMENT 'Идентификатор  склада по умолчанию [ROOMS.RID]'
,TS_KATEG	INTEGER	NULL COMMENT 'Категория  сложности:101 - высшая категория102 - первая категория103 - вторая категория104 - дневной стационар'
,WARELIST	VARCHAR(1024)	NULL COMMENT 'Доступные  склады (список [ROOMS.RID], разделенных запятыми)'
,REPL$ID	INTEGER	NULL COMMENT 'Идентификатор  репликации'
,REPL$GRPID	INTEGER	NULL COMMENT 'Идентификатор  группы репликации'
,DAYHOSPLASTDAYTYPE	INTEGER NULL  COMMENT 'Учет последних суток (для расчета числа койко-дней в дневном  стационаре):0 - не учитывать последние сутки пребывания в отделении1 - учитывать последние сутки пребывания в отделении'
,DAYHOSPCOUNTDAYTYPE	INTEGER	NULL COMMENT 'Учет типов дней (для расчета числа койко-дней в дневном стационаре):0 - учитывать все дни пребывания в  отделении1 - учитывать все дни пребывания в  отделении, кромевыходных2 - учитывать все дни пребывания в  отделении, кромевоскресений и праздников'
,DISDATE	DATETIME	NULL COMMENT 'Дата,  начиная с которой стационарное отделение не используется'
,CHECKNEWBORN	INTEGER	NULL COMMENT 'Используется ли в стационарном отделении регистрация новорожденных:1 - да,  используется0 -  нет, не используется'
,OID	VARCHAR(255)	NULL COMMENT 'Идентификатор  стационарного отделения в ФРМО'
,PALLIATIVEDEP	INTEGER	NULL COMMENT 'Является  ли стационарное отделение паллиативным:1 - да,  является0 или  null - нет, не является'
,VISITTIMEFROM VARCHAR(50)	NULL
,VISITTIMETO VARCHAR(50)	NULL
    , load_id BIGINT NULL
    , valid_from_dttm DATETIME NULL DEFAULT '1980-12-31 00:00'
    , valid_to_dttm DATETIME NULL DEFAULT '9999-12-31 23:59'  
    -- , extract_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    -- , src_chng_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    , src_sys_id VARCHAR(10) NULL DEFAULT 'mis'
    , op VARCHAR(10) NULL
    , ts_ms BIGINT NULL
    , source_ts_ms BIGINT NULL 
	, deleted_flg varchar(1) NULL DEFAULT 'N'   
    , extract_dttm DATETIME as  cast(from_unixtime(ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)   
    , src_chng_dttm DATETIME  as  cast(from_unixtime(source_ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)       	
) ENGINE=OLAP 
PRIMARY KEY(SDEPID, load_dttm) 
DISTRIBUTED BY hash(SDEPID) buckets 1
order by (SDEPID, load_dttm) 
PROPERTIES (
"bucket_size" = "4294967296",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"fast_schema_evolution" = "true"
);

-- ============================================================================
-- 7. DOCTOR
-- ============================================================================

drop TABLE if exists unverified_mart.medical__dm_DOCTOR_dim;
CREATE TABLE medical__dm_DOCTOR_dim (
    DCODE BIGINT NOT NULL COMMENT 'Идентификатор доктора (первичный ключ)',
    load_dttm DATETIME  DEFAULT CURRENT_TIMESTAMP, 
    FILIAL INT NULL COMMENT 'Идентификатор филиала. [FILIALS.FILID]',
    DNAME VARCHAR(48) NULL COMMENT 'Имя',
    DPHONE1 VARCHAR(24) NULL COMMENT 'Рабочий телефон',
    DPHONE2 VARCHAR(24) NULL COMMENT 'Домашний телефон',
    DPASSWRD VARCHAR(128) NULL COMMENT 'Пароль',
    DRIGHTS INT NULL COMMENT 'не используется',
    STDTYPE BIGINT NULL COMMENT 'Идентификатор типа конфигурации. [CONFTYPE.STDTYPE]',
    FULLNAME VARCHAR(80) NULL COMMENT 'Полное имя',
    TERAP VARCHAR(1) NULL COMMENT 'не используется',
    HIRURG VARCHAR(1) NULL COMMENT 'не используется',
    PARODONT VARCHAR(1) NULL COMMENT 'не используется',
    ORTOPED VARCHAR(1) NULL COMMENT 'не используется',
    ORTODONT VARCHAR(1) NULL COMMENT 'не используется',
    RNUM BIGINT NULL COMMENT 'Идентификатор кабинета. [ROOMS.RID]',
    ASSISTCODE BIGINT NULL COMMENT 'Идентифткатор ассистента. [DOCTOR.DCODE]',
    DOCTCODE VARCHAR(24) NULL COMMENT 'Код доктора',
    MECHANIC BIGINT NULL COMMENT 'Идентифткатор техника. [DOCTOR.DCODE]',
    LOCKED SMALLINT NULL COMMENT 'Флаг "Уволен". 1 - Да',
    DEPNUM BIGINT NULL COMMENT 'Идентификатор отделения. [DEPARTMENTS.DEPNUM]',
    SALARYTYPE INT NULL COMMENT 'не используется',
    PERSONTYPE INT NULL COMMENT 'Флаг "Подотчетное лицо". 1 - Да Редактируется в справочнике персонала на закладке Основные реквизиты, в группе Место работы',
    CHAIR BIGINT NULL COMMENT 'Идентификатор кресла [CHAIRS.CHID]',
    ASSISTCHK SMALLINT NULL COMMENT 'Флаг "Работает ассистентом" 1 - Да',
    INCMODE SMALLINT NULL COMMENT 'Не используется',
    WORKWITHFILIAL SMALLINT NULL COMMENT 'Флаг "Работа с филиалами в расписании". 1 - Да',
    VIEWINSCHED SMALLINT NULL COMMENT 'Отображать в расписании: 0 - Не отображать 1 - Расписание 2 - Операционное расписание 10 - Везде',
    EXTPCODE BIGINT NULL COMMENT 'Идентификатор пациента. [CLIENTS.PCODE]',
    USESTAVKA SMALLINT NULL COMMENT 'Флаг "Работает на ставке". 1 - да',
    USEINNARAD SMALLINT NULL COMMENT 'Выполняет заказ-наряды. [N_NARADTYPES.NARID]',
    ASDSPECCODE BIGINT NULL COMMENT 'Специализация для аналитики. [SPECIALITY.SCODE]',
    MPHONE VARCHAR(24) NULL COMMENT 'Мобильный телефон',
    SHORTTALON INT NULL COMMENT 'Вид талона (печать талона в расписании): 0 - стандартный 1 - Сокращенный 2 - С расширенным списком услуг',
    SPECSERV BIGINT NULL COMMENT 'Спец. условия обслуживания по умолчанию. [DISCOUNTSPRAV.DID]',
    ASSISTTYPE INT NULL COMMENT 'Наличие ассистента: 1 - Есть ассистент 2 - Обязателен (не используется) 3 - Несколько ассистентов 4 - Нет ассистента',
    JID BIGINT NULL COMMENT 'Юр. лицо (осн. место работы). [JPERSONS.JID]',
    SCHEDCHAIRS INT NULL COMMENT 'Работа с расписанием: 1 - № кресла - из графика работы 2 - Возможно изменение № кресла при назначении',
    MATRESP INT NULL COMMENT 'Флаг "Подотчетное лицо". 1 - Да Редактируется в справочнике персонала на закладке Дополнительно, в группе Учет материалов',
    GRSTREET VARCHAR(255) NULL COMMENT 'Идентификаторы территориальных участков (через запятую) [STREETREF.STID]',
    GRSDOCT INT NULL COMMENT 'Флаг "Участковый доктор". 1 - Да',
    SCHEDCOMMENT VARCHAR(255) NULL COMMENT 'Содержится в назначении Строка строилась как "[DOCTOR.WORKDATE] [DOCTOR.FULLNAME]"',
    INVENTCODE INT NULL COMMENT 'Номер инвентаризации',
    INVENTDATE DATETIME NULL COMMENT 'Дата инвентаризации',
    ACCESSLEVEL INT NULL COMMENT 'Уровень доступа',
    SHEDCARDKEY VARCHAR(64) NULL COMMENT 'Код карты сотрудника',
    KATEG BIGINT NULL COMMENT 'Категория приема по умолчанию. [CLGROUP.GRCOD]',
    CALLCENTRE INT NULL COMMENT 'Флаг "Сотрудник Call-центра". 1 - Да',
    LOCKDATE DATETIME NULL COMMENT 'Дата увольнения',
    CASHCODE INT NULL COMMENT 'Код кассира',
    TEXTTOSCHED VARCHAR(128) NULL COMMENT 'Доп. текст в расписание',
    FULLNAME2 VARCHAR(80) NULL COMMENT 'Полное имя на иностранном языке',
    MATPERSRESP INT NULL COMMENT 'Флаг "Лично матер. ответст.". 1 - Да',
    DOCTPOST VARCHAR(500) NULL COMMENT 'Должность',
    BDATE DATETIME NULL COMMENT 'Дата рождения',
    SCHID BIGINT NULL COMMENT 'Плановая услуга для первичных назначений. [WSCHEMA.SCHID]',
    ENTERDATE DATETIME NULL COMMENT 'Дата поступления на работу',
    INSTAT INT NULL COMMENT 'Флаг "Работает в стационаре". 1 - Да',
    NAZNCOUNT INT NULL COMMENT 'Информировать о плановых назначениях (кол-во назначений)',
    USERID INT NULL COMMENT 'Внешний идентификатор пользователя (ПИД для факт.расписания Anviz) Генератор [USERID_GEN]',
    SCHEDRAZDEL BIGINT NULL COMMENT 'Раздел расписания по умолчанию. [SHEDMARKS.MRKID]',
    SCHEDINFO VARCHAR(2048) NULL COMMENT 'Расширенный комментарий для расписания',
    BT_PROFILE BIGINT NULL COMMENT 'Профиль учета рабочего времени из [BT_FACT_PROFILE.PID]',
    GRSVID BIGINT NULL COMMENT 'Идентификатор вида участкового обслуживания [GRSSERVICE.GRSVID]',
    BT_ENTER_PROFILE BIGINT NULL COMMENT 'Профиль доступа из [BT_DOOR_PROFILE.DPID] (Не используется)',
    BT_USE SMALLINT NULL COMMENT '1 - сотрудник используется при учете рабочего времени',
    BT_PASSWORD VARCHAR(8) NULL COMMENT 'Пароль доступа при учете рабочего времени',
    SHINTERV INT NULL COMMENT 'Стандартный интервал приема (мин)',
    STEPEN BIGINT NULL COMMENT 'Ученая степень. [SAL_SCSTEPEN.ROWCODE]',
    ZVANIE BIGINT NULL COMMENT 'Ученое звание.[SAL_SCZVANIE.ROWCODE]',
    SCHEDRESERV BIGINT NULL COMMENT 'Автом. резервирование времени при одновр. работе в нескольких креслах. [SHEDMARKS.MRKID]',
    OBSLADD_DIS INT NULL COMMENT 'Флаг "Запрет на добавление обследов. в картотеке". 1 - Да',
    RESPONS INT NULL COMMENT 'Флаг "Ответственный (учет Контрагентов)". 1 - Да',
    NTUSER VARCHAR(64) NULL COMMENT 'Пользователь Windows',
    KEYPROFILE VARCHAR(8) NULL COMMENT 'Профиль клавиатуры (ввод анализов)',
    GRS_KOEF DOUBLE NULL COMMENT 'Коэффициент участкового обслуживания',
    CLMAIL VARCHAR(64) NULL COMMENT 'Адрес e-mail',
    SYSTYPE INT NULL COMMENT '1 - системный пользователь',
    PROFID BIGINT NULL COMMENT 'Должность из справочника [PROFESSION.PROFID]',
    CHAIRMAN INT NULL COMMENT '1 - председатель ВК',
    COSTTYPE INT NULL COMMENT 'Склад. Расчет цены: 0 - Средняя 1 - ФИФО',
    SEX BIGINT NULL COMMENT 'Пол сотрудника 1 - мужской 2 - женский',
    FCFSWORKTYPE SMALLINT NULL COMMENT 'Работа с живой очередью: 0 - не используется 1 - может использоваться 2 - используется по умолчанию',
    VIEWINWEB SMALLINT NULL COMMENT '0 - По умолчанию для сайта, 1 - Отображать всегда, 2 - Не отображать',
    SCHEDGRIDTYPE SMALLINT NULL COMMENT 'Тип сетки расписания: 0 - Стандартная; 1 - Фиксированная;',
    WEBLOGIN VARCHAR(64) NULL COMMENT 'Логин для WEB',
    SNILS VARCHAR(24) NULL COMMENT 'СНИЛС врача',
    PARTTYPE INT NULL COMMENT 'Не используется',
    SMTID BIGINT NULL COMMENT 'Шаблон резерва по умолчанию [SCHEDMARKTEMPL.SMTID]',
    DOCTCODE_REG VARCHAR(24) NULL COMMENT 'Код по регистру медработников',
    CALLUSERID VARCHAR(1024) NULL COMMENT 'Идентификатор пользователя в системе телефонии для Стандартного протокола и/или SIP пользователь',
    PHONEINT VARCHAR(64) NULL COMMENT 'Внутренний телефон оператора в системе телефонии',
    REPL$ID INT NULL COMMENT 'Идентификатор репликации',
    REPL$GRPID INT NULL COMMENT 'Идентификатор группы репликации',
    NAPRAVREQ SMALLINT NULL COMMENT '1 - обязательно указание направления',
    CASHNAME VARCHAR(255) NULL COMMENT 'Имя кассира (для отдельной печати)',
    LASTTREATFILDAY INT NULL COMMENT 'Последнее посещение ЛПУ, дней',
    LASTTREATDEPDAY INT NULL COMMENT 'Последнее посещение отделения, дней',
    LASTTREATDOCDAY INT NULL COMMENT 'Последнее посещение врача, дней',
    MODIFYDATE DATETIME NULL COMMENT 'Дата изменения',
    CALLUSERPASSWORD VARCHAR(64) NULL COMMENT 'Пароль SIP-учетки',
    CALLSYSUSER VARCHAR(64) NULL COMMENT 'Имя пользователя в системе телефонии Инфрател',
    CALLSYSPASSWORD VARCHAR(64) NULL COMMENT 'Пароль в системе телефонии Инфрател',
    SCHED_FILIAL_GROUP_ID BIGINT NULL COMMENT 'Группа филиалов [FILIAL_GROUPS.FILIAL_GROUP_ID]',
    WEBNAME VARCHAR(128) NULL COMMENT 'ФИО врача для сайта',
    DOCTQUALIFYID BIGINT NULL COMMENT 'Квалификация сотрудника [DICINFO.DICID {REFID = -10042}]',
    MEDIAID VARCHAR(64) NULL COMMENT 'ключ медиарепозитория по которому можно универсально получить расширенное описание доктора',
    WEBCOMMENT VARCHAR(255) NULL COMMENT 'Краткое описание для сайта',
    DOCTCODE_LGOT VARCHAR(24) NULL COMMENT 'Код по регистру ДЛО',
    DPASSWRDDATE DATETIME NULL COMMENT 'Время изменения пароля',
    POADATA VARCHAR(255) NULL COMMENT 'Доверенность для документов',
    ONLINEUSERID VARCHAR(128) NULL COMMENT 'Логин пользователя для онлайн приёмов',
    DJRESPONS INT NULL COMMENT 'ответственный в филиале за ведение реестра обезболивания (0 - нет, 1 -да)',
    EXTGUID VARCHAR(255) NULL COMMENT 'Внешний GUID для интеграции в другие системы',
    SHOWCBJOURNAL INT NULL COMMENT 'Признак для председателя ВК "Открывать журнал ЛН при входе в ИК"',
    INN VARCHAR(12) NULL COMMENT 'ИНН',
    USEKATEGTYPE INT NULL COMMENT 'Учитывать категорию по умолчанию для б/нал пациентов. 0 - не учитывать, 1 - учитывать',
    TRADEROOM BIGINT NULL COMMENT 'Склад для торгового наряда по умолчанию [rooms.rid]',
    ELQUSER INT NULL COMMENT 'признак того что пользователь работает в электронной очереди',
    ELQLOGIN VARCHAR(64) NULL COMMENT 'логин пользователя в системе управления очередью',
    ELQPWD VARCHAR(64) NULL COMMENT 'пароль пользователя в системе управления очередью',
    STAFFDEPID BIGINT NULL COMMENT 'Подразделение по штатному расписанию для доктора [DICINFO.DICID {REFID = -101}]',
    MULTIDEP INT NULL COMMENT 'Работает в нескольких отделениях, 1 = да',
    DNAMEFORBULL VARCHAR(48) NULL COMMENT 'ФИО доктора для больничного листа',
    STARTREPID BIGINT NULL COMMENT 'Стартовый дашборд. [MREPORTS.REPID]',
    USER_ID_ALGOM VARCHAR(128) NULL COMMENT 'Идентификатор пользователя Алгом',
    ISONKOSPEC INT NULL COMMENT 'Признак что специалист онколог',
    PASS_CHANGE_REQUIRE INT NULL COMMENT 'Флаг "Одноразовый пароль при авторизации". 1 - Да',
    BAGE INT NULL COMMENT 'Минимальный возраст пациента',
    FAGE INT NULL COMMENT 'Максимальный возраст пациента',
    AGEUNITTYPE INT NULL COMMENT 'Единица измерения возраста 0 - Лет 1 - Месяцев 2 - Дней',
    POL INT NULL COMMENT 'Пол 0 - не задан 1 - мужской 2 - женский',
    CDSSUSERCODE VARCHAR(48) NULL COMMENT 'Код пользователя в сервисе скрининга "Медиката"',
    CHATUSER VARCHAR(64) NULL COMMENT 'Логин оператора чата',
    CHATPASSWORD VARCHAR(64) NULL COMMENT 'Пароль оператора чата',
    IVFSPEC BIGINT NULL COMMENT 'специалист, участвующий в ЭКО',
    FRMR_MODIFYDATE DATETIME NULL COMMENT 'Дата изменения записи в ФРМР',
    FRMR_OID VARCHAR(64) NULL COMMENT 'ОИД в ФРМР',
    FRMR_STATUS INT NULL COMMENT 'Статус синхронизации с ФРМР [dicinfo.dicid {refid = -512}]',
    FRMR_DATE DATETIME NULL COMMENT 'Дата синхронизации с ФРМР',
    ASSISTREQUIRED INT NULL COMMENT 'признак Обязательность ассистента в приеме',
    BRIGTRANSLASTTREAT INT NULL COMMENT 'Перенос бригады с предыдущего амбулаторного приема: 0 - Нет 1 - Да, с подтверждением 2 - Да, без подтверждения',
    ISAGROWNER INT NULL COMMENT 'Признак владельца АГ',
    FEED_YANDEX_UPLOAD INT NULL COMMENT 'Признак загрузки отзывов в Яндекс',
    HEADERDOC_GENITIVE VARCHAR(48) NULL COMMENT 'ФИО врача в родительном падеже для заголовка',
    DOCTPOST_GENITIVE VARCHAR(128) NULL COMMENT 'Должность в родительном падеже',
    DNAME_GENITIVE VARCHAR(48) NULL COMMENT 'Имя в родительном падеже'
    , EXTJID BIGINT NULL -- add
    , load_id BIGINT NULL
    , valid_from_dttm DATETIME NULL DEFAULT '1980-12-31 00:00'
    , valid_to_dttm DATETIME NULL DEFAULT '9999-12-31 23:59'  
    -- , extract_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    -- , src_chng_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    , src_sys_id VARCHAR(10) NULL DEFAULT 'mis'
    , op VARCHAR(10) NULL
    , ts_ms BIGINT NULL
    , source_ts_ms BIGINT NULL 
	, deleted_flg varchar(1) NULL DEFAULT 'N'   
    , extract_dttm DATETIME as  cast(from_unixtime(ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)   
    , src_chng_dttm DATETIME  as  cast(from_unixtime(source_ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)      	
) ENGINE=OLAP 
PRIMARY KEY(DCODE, load_dttm) 
DISTRIBUTED BY hash(DCODE) buckets 1
order by (DCODE, load_dttm) 
PROPERTIES (
"bucket_size" = "4294967296",
"in_memory" = "false",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"fast_schema_evolution" = "true"
);



-- ============================================================================
-- 7.BUILDINGS
-- ============================================================================

drop TABLE if exists unverified_mart.medical__dm_BUILDINGS_dim;
CREATE TABLE `medical__dm_BUILDINGS_dim` (
`IDBUILD` bigint(20) NOT NULL COMMENT "Идентификатор корпуса. Генератор [SPRAV_GEN]",
    load_dttm DATETIME  DEFAULT CURRENT_TIMESTAMP, 
  `FILIAL` int(11) NULL COMMENT "Филиал корпуса [FILIALS.FILID]",
  `FRMOID` varchar(40) NULL COMMENT "Идентификатор корпуса в ФРМО",
  `JID` bigint(20) NULL COMMENT "Идентификатор юридического лица JPERSONS.JID",
  `ADDRFROMJPERSONS` smallint(6) NULL COMMENT "признак Совпадает с адресом юр. лица",
  `REPL$GRPID` int(11) NULL COMMENT "Идентификатор группы репликации",
  `AVGROUTETIME` int(11) NULL COMMENT "Среднее время перемещения между любыми этажами (для расчета маршрутов) в секундах",
  `REPL$ID` int(11) NULL COMMENT "Идентификатор репликации",  
  `ISNONUNIQUEADDRESS` int(11) NULL COMMENT "признак По адресу располагается более одного здания",
  `BNAME` varchar(256) NULL COMMENT "Название корпуса."
    , load_id BIGINT NULL
    , valid_from_dttm DATETIME NULL DEFAULT '1980-12-31 00:00'
    , valid_to_dttm DATETIME NULL DEFAULT '9999-12-31 23:59'  
    -- , extract_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    -- , src_chng_dttm DATETIME NULL DEFAULT CURRENT_TIMESTAMP
    , src_sys_id VARCHAR(10) NULL DEFAULT 'mis'
    , op VARCHAR(10) NULL
    , ts_ms BIGINT NULL
    , source_ts_ms BIGINT NULL 
	, deleted_flg varchar(1) NULL DEFAULT 'N'   
    , extract_dttm DATETIME as  cast(from_unixtime(ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)   
    , src_chng_dttm DATETIME  as  cast(from_unixtime(source_ts_ms/1000000, '%Y-%m-%d %H:%i:%s') as datetime)    
   
) ENGINE=OLAP 
PRIMARY KEY(IDBUILD, load_dttm)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`IDBUILD`) BUCKETS 1 
order by (IDBUILD, load_dttm) 
PROPERTIES (
"compression" = "LZ4",
"datacache.enable" = "true",
"enable_async_write_back" = "false",
"enable_persistent_index" = "true",
"persistent_index_type" = "CLOUD_NATIVE",
"replication_num" = "1",
"storage_volume" = "builtin_storage_volume"
);

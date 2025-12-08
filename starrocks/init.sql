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
-- ============================================================================
-- Таблицы для загрузки данных из Kafka топиков ESUD
-- ============================================================================

-- Таблица stg_organizations для организаций из топика sys__nsi__esud_rzdm__organization__data
CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_organizations (
    UID VARCHAR COMMENT 'UID',
    GUID VARCHAR NOT NULL COMMENT 'GUID',
    UIDSearchString VARCHAR COMMENT 'UIDSearchString',
    IndividualnyyPredprinimatel VARCHAR COMMENT 'IndividualnyyPredprinimatel',
    rc_Audit VARCHAR COMMENT 'rc_Audit',
    rc_Auditor VARCHAR COMMENT 'rc_Auditor',
    rc_VidInostrannoyStruktury VARCHAR COMMENT 'rc_VidInostrannoyStruktury',
    rc_VidOrganizatsiiPoUmolchaniyu VARCHAR COMMENT 'rc_VidOrganizatsiiPoUmolchaniyu',
    rc_VidOtkloneniyaKontroliruemogoZnacheniya VARCHAR COMMENT 'rc_VidOtkloneniyaKontroliruemogoZnacheniya',
    rc_GlavnyyBukhgalterFIO VARCHAR COMMENT 'rc_GlavnyyBukhgalterFIO',
    rc_GorodStranaMezhdunarodnyy VARCHAR COMMENT 'rc_GorodStranaMezhdunarodnyy',
    rc_GruppaKontragenta VARCHAR COMMENT 'rc_GruppaKontragenta',
    rc_DataPrekrashcheniyaDeyatelnosti VARCHAR COMMENT 'rc_DataPrekrashcheniyaDeyatelnosti',
    rc_Divizion VARCHAR COMMENT 'rc_Divizion',
    rc_ZakupkaPoFZ223 BOOLEAN COMMENT 'rc_ZakupkaPoFZ223',
    rc_InostrannayaStrukturaBezObrazovaniyaYurLitsa BOOLEAN COMMENT 'rc_InostrannayaStrukturaBezObrazovaniyaYurLitsa',
    rc_InostrannyyNalogovyyRezident BOOLEAN COMMENT 'rc_InostrannyyNalogovyyRezident',
    rc_IPImya VARCHAR COMMENT 'rc_IPImya',
    rc_IPOtchestvo VARCHAR COMMENT 'rc_IPOtchestvo',
    rc_IPFamiliya VARCHAR COMMENT 'rc_IPFamiliya',
    rc_IspolzovatStrokuPlanaZakupok BOOLEAN COMMENT 'rc_IspolzovatStrokuPlanaZakupok',
    rc_KodInykhVidovDeyatelnostiNFO VARCHAR COMMENT 'rc_KodInykhVidovDeyatelnostiNFO',
    rc_KodNalogoplatelshchikaInostrannyy VARCHAR COMMENT 'rc_KodNalogoplatelshchikaInostrannyy',
    rc_KodOsnovnogoVidaDeyatelnostiNFO VARCHAR COMMENT 'rc_KodOsnovnogoVidaDeyatelnostiNFO',
    rc_Makroregion VARCHAR COMMENT 'rc_Makroregion',
    rc_NaimenovanieBE VARCHAR COMMENT 'rc_NaimenovanieBE',
    rc_NaimenovanieInykhVidovDeyatelnostiNFO VARCHAR COMMENT 'rc_NaimenovanieInykhVidovDeyatelnostiNFO',
    rc_NaimenovanieOrganizatsiiNaAngliyskomYazyke VARCHAR COMMENT 'rc_NaimenovanieOrganizatsiiNaAngliyskomYazyke',
    rc_NaimenovanieOsnovnogoVidaDeyatelnostiNFO VARCHAR COMMENT 'rc_NaimenovanieOsnovnogoVidaDeyatelnostiNFO',
    rc_NaimenovanieRegistriruyushchegoOrgana VARCHAR COMMENT 'rc_NaimenovanieRegistriruyushchegoOrgana',
    rc_NekreditnayaFinansovayaOrganizatsiya BOOLEAN COMMENT 'rc_NekreditnayaFinansovayaOrganizatsiya',
    rc_OrganizatsiyaDeystvuyushchaya BOOLEAN COMMENT 'rc_OrganizatsiyaDeystvuyushchaya',
    rc_OsnovaniePrekrashcheniyaDeyatelnosti VARCHAR COMMENT 'rc_OsnovaniePrekrashcheniyaDeyatelnosti',
    rc_OsnovnoyBankovskiySchet VARCHAR COMMENT 'rc_OsnovnoyBankovskiySchet',
    rc_PoligonZhD VARCHAR COMMENT 'rc_PoligonZhD',
    rc_RegionRossii VARCHAR COMMENT 'rc_RegionRossii',
    rc_RegistriruyushchiyOrgan VARCHAR COMMENT 'rc_RegistriruyushchiyOrgan',
    rc_RukovoditelOrganizatsiiDolzhnost VARCHAR COMMENT 'rc_RukovoditelOrganizatsiiDolzhnost',
    rc_RukovoditelOrganizatsiiFIO VARCHAR COMMENT 'rc_RukovoditelOrganizatsiiFIO',
    rc_SvedeniyaOLitsenzii VARCHAR COMMENT 'rc_SvedeniyaOLitsenzii',
    rc_TarifnayaZona VARCHAR COMMENT 'rc_TarifnayaZona',
    rc_FormatTsFO VARCHAR COMMENT 'rc_FormatTsFO',
    rc_FRMO_OrgID VARCHAR COMMENT 'rc_FRMO_OrgID',
    rc_EtoBank BOOLEAN COMMENT 'rc_EtoBank',
    rc_EtoIndividualnyyPredprinimatel BOOLEAN COMMENT 'rc_EtoIndividualnyyPredprinimatel',
    rc_EtoMezhdunarodnyyKholding BOOLEAN COMMENT 'rc_EtoMezhdunarodnyyKholding',
    rc_EtoNKO BOOLEAN COMMENT 'rc_EtoNKO',
    rc_EtoSamozanyatyy BOOLEAN COMMENT 'rc_EtoSamozanyatyy',
    rc_EtoStrakhovayaKompaniya BOOLEAN COMMENT 'rc_EtoStrakhovayaKompaniya',
    rc_AdresMezhdunarodnyy VARCHAR COMMENT 'rc_AdresMezhdunarodnyy',
    rc_KodOKVED2 VARCHAR COMMENT 'rc_KodOKVED2',
    VidObmenaSKontroliruyushchimiOrganami VARCHAR COMMENT 'VidObmenaSKontroliruyushchimiOrganami',
    Voenkomat VARCHAR COMMENT 'Voenkomat',
    ValyutaReglamentirovannogoUcheta VARCHAR COMMENT 'ValyutaReglamentirovannogoUcheta',
    GrafikRaboty VARCHAR COMMENT 'GrafikRaboty',
    DataRegistratsii VARCHAR COMMENT 'DataRegistratsii',
    DopolnitelnyyKodFSS VARCHAR COMMENT 'DopolnitelnyyKodFSS',
    DopuskayutsyaVzaimoraschetyCherezGolovnuyuOrganizatsiyu BOOLEAN COMMENT 'DopuskayutsyaVzaimoraschetyCherezGolovnuyuOrganizatsiyu',
    EstObosoblennyePodrazdeleniya BOOLEAN COMMENT 'EstObosoblennyePodrazdeleniya',
    ZaregistrirovanVOEZ BOOLEAN COMMENT 'ZaregistrirovanVOEZ',
    INN VARCHAR COMMENT 'INN',
    KPP VARCHAR COMMENT 'KPP',
    IPKodPodchinennostiFSS VARCHAR COMMENT 'IPKodPodchinennostiFSS',
    IPRegistratsionnyyNomerPFR VARCHAR COMMENT 'IPRegistratsionnyyNomerPFR',
    IPRegistratsionnyyNomerTFOMS VARCHAR COMMENT 'IPRegistratsionnyyNomerTFOMS',
    IPRegistratsionnyyNomerFSS VARCHAR COMMENT 'IPRegistratsionnyyNomerFSS',
    KodVStraneRegistratsii VARCHAR COMMENT 'KodVStraneRegistratsii',
    KodNalogovogoOrgana VARCHAR COMMENT 'KodNalogovogoOrgana',
    KodNalogovogoOrganaPoluchatelya VARCHAR COMMENT 'KodNalogovogoOrganaPoluchatelya',
    KodOKVED VARCHAR COMMENT 'KodOKVED',
    KodOKVED2 VARCHAR COMMENT 'KodOKVED2',
    KodOKOPF VARCHAR COMMENT 'KodOKOPF',
    KodOKFS VARCHAR COMMENT 'KodOKFS',
    KodOrganaPFR VARCHAR COMMENT 'KodOrganaPFR',
    KodOrganaFSGS VARCHAR COMMENT 'KodOrganaFSGS',
    KodPodchinennostiFSS VARCHAR COMMENT 'KodPodchinennostiFSS',
    KodPoOKATO VARCHAR COMMENT 'KodPoOKATO',
    KodPoOKPO VARCHAR COMMENT 'KodPoOKPO',
    Kontragent VARCHAR COMMENT 'Kontragent',
    NaimenovanieSokrashchennoe VARCHAR COMMENT 'NaimenovanieSokrashchennoe',
    KrupneyshiyNalogoplatelshchik BOOLEAN COMMENT 'KrupneyshiyNalogoplatelshchik',
    NaimenovanieInostrOrganizatsii VARCHAR COMMENT 'NaimenovanieInostrOrganizatsii',
    OGRN VARCHAR COMMENT 'OGRN',
    NaimenovanieNalogovogoOrgana VARCHAR COMMENT 'NaimenovanieNalogovogoOrgana',
    Prefiks VARCHAR COMMENT 'Prefiks',
    NaimenovanieTerritorialnogoOrganaPFR VARCHAR COMMENT 'NaimenovanieTerritorialnogoOrganaPFR',
    NaimenovanieTerritorialnogoOrganaFSS VARCHAR COMMENT 'NaimenovanieTerritorialnogoOrganaFSS',
    NeZapolnyatPodrazdeleniyaVMeropriyatiyakhTrudovoyDeyatelnosti BOOLEAN COMMENT 'NeZapolnyatPodrazdeleniyaVMeropriyatiyakhTrudovoyDeyatelnosti',
    ObmenKatalogOtpravkiDannykhOtchetnosti VARCHAR COMMENT 'ObmenKatalogOtpravkiDannykhOtchetnosti',
    ObmenKatalogProgrammyElektronnoyPochty VARCHAR COMMENT 'ObmenKatalogProgrammyElektronnoyPochty',
    ObmenKodAbonenta VARCHAR COMMENT 'ObmenKodAbonenta',
    ObosoblennoePodrazdelenie BOOLEAN COMMENT 'ObosoblennoePodrazdelenie',
    GolovnayaOrganizatsiya VARCHAR COMMENT 'GolovnayaOrganizatsiya',
    OtdelenieInostrannoyOrganizatsii BOOLEAN COMMENT 'OtdelenieInostrannoyOrganizatsii',
    PolnoeNaimenovanie VARCHAR COMMENT 'PolnoeNaimenovanie',
    NaimenovanieMezhdunarodnoe VARCHAR COMMENT 'NaimenovanieMezhdunarodnoe',
    PrimenyatRayonnyyKoeffitsient BOOLEAN COMMENT 'PrimenyatRayonnyyKoeffitsient',
    PrimenyatSevernuyuNadbavku BOOLEAN COMMENT 'PrimenyatSevernuyuNadbavku',
    ProtsentSevernoyNadbavki BIGINT COMMENT 'ProtsentSevernoyNadbavki',
    RayonnyyKoeffitsient BIGINT COMMENT 'RayonnyyKoeffitsient',
    RayonnyyKoeffitsientRF BIGINT COMMENT 'RayonnyyKoeffitsientRF',
    RegistratsionnyyNomerPFR VARCHAR COMMENT 'RegistratsionnyyNomerPFR',
    RegistratsionnyyNomerTFOMS VARCHAR COMMENT 'RegistratsionnyyNomerTFOMS',
    RegistratsionnyyNomerFSS VARCHAR COMMENT 'RegistratsionnyyNomerFSS',
    RegistratsiyaVNalogovomOrgane VARCHAR COMMENT 'RegistratsiyaVNalogovomOrgane',
    SvidetelstvoDataVydachi VARCHAR COMMENT 'SvidetelstvoDataVydachi',
    SvidetelstvoSeriyaNomer VARCHAR COMMENT 'SvidetelstvoSeriyaNomer',
    StranaPostoyannogoMestonakhozhdeniya VARCHAR COMMENT 'StranaPostoyannogoMestonakhozhdeniya',
    StranaRegistratsii VARCHAR COMMENT 'StranaRegistratsii',
    StranaRegistratsiiInostrannoyOrganizatsii VARCHAR COMMENT 'StranaRegistratsiiInostrannoyOrganizatsii',
    UchetnayaZapisObmena VARCHAR COMMENT 'UchetnayaZapisObmena',
    FaylLogotip VARCHAR COMMENT 'FaylLogotip',
    FaylFaksimilnayaPechat VARCHAR COMMENT 'FaylFaksimilnayaPechat',
    TsifrovoyIndeksObosoblennogoPodrazdeleniya BIGINT COMMENT 'TsifrovoyIndeksObosoblennogoPodrazdeleniya',
    YuridicheskoeFizicheskoeLitso VARCHAR COMMENT 'YuridicheskoeFizicheskoeLitso',
    YurFizLitso VARCHAR COMMENT 'YurFizLitso',
    rc_NaimenovanieOKVED2 VARCHAR COMMENT 'rc_NaimenovanieOKVED2',
    rc_KodOKTMO VARCHAR COMMENT 'rc_KodOKTMO',
    rc_NaimenovanieOKVED VARCHAR COMMENT 'rc_NaimenovanieOKVED',
    rc_NaimenovanieOKOPF VARCHAR COMMENT 'rc_NaimenovanieOKOPF',
    rc_NaimenovanieOKFS VARCHAR COMMENT 'rc_NaimenovanieOKFS',
    MomentOfTime DATETIME COMMENT 'MomentOfTime',
    load_timestamp DATETIME COMMENT 'Время загрузки'
);

-- Таблица stg_contractors для контрагентов из топика sys__nsi__esud_rzdm__contractor__data
CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_contractors (
    UID VARCHAR COMMENT 'UID',
    GUID VARCHAR NOT NULL COMMENT 'GUID',
    UIDSearchString VARCHAR COMMENT 'UIDSearchString',
    rc_AdresMezhdunarodnyy VARCHAR COMMENT 'rc_AdresMezhdunarodnyy',
    rc_VidGosudarstvennogoOrgana VARCHAR COMMENT 'rc_VidGosudarstvennogoOrgana',
    rc_VidKontragentaDlyaVGO VARCHAR COMMENT 'rc_VidKontragentaDlyaVGO',
    rc_GorodStranaMezhdunarodnyy VARCHAR COMMENT 'rc_GorodStranaMezhdunarodnyy',
    rc_GosudarstvennyyOrgan BOOLEAN COMMENT 'rc_GosudarstvennyyOrgan',
    rc_GruppaKontragenta VARCHAR COMMENT 'rc_GruppaKontragenta',
    rc_DataPostanovkiNaUchetVNalogovoy VARCHAR COMMENT 'rc_DataPostanovkiNaUchetVNalogovoy',
    rc_DataRozhdeniya VARCHAR COMMENT 'rc_DataRozhdeniya',
    rc_DULNomer VARCHAR COMMENT 'rc_DULNomer',
    rc_DULSeriya VARCHAR COMMENT 'rc_DULSeriya',
    rc_Imya VARCHAR COMMENT 'rc_Imya',
    rc_IndividualnyyPredprinimatel BOOLEAN COMMENT 'rc_IndividualnyyPredprinimatel',
    rc_IspolzovatProgrammuZakupok BOOLEAN COMMENT 'rc_IspolzovatProgrammuZakupok',
    rc_KanalProdazh VARCHAR COMMENT 'rc_KanalProdazh',
    rc_KodGosudarstvennogoOrgana VARCHAR COMMENT 'rc_KodGosudarstvennogoOrgana',
    rc_KodOKVED VARCHAR COMMENT 'rc_KodOKVED',
    rc_KodOKVED2 VARCHAR COMMENT 'rc_KodOKVED2',
    rc_KodOKFS VARCHAR COMMENT 'rc_KodOKFS',
    rc_KodPoOKTMO VARCHAR COMMENT 'rc_KodPoOKTMO',
    rc_OsnovnoeKontaktnoeLitso VARCHAR COMMENT 'rc_OsnovnoeKontaktnoeLitso',
    rc_OsnovnoeKontaktnoeLitsoVIFNS VARCHAR COMMENT 'rc_OsnovnoeKontaktnoeLitsoVIFNS',
    rc_OsnovnoyBankovskiySchet VARCHAR COMMENT 'rc_OsnovnoyBankovskiySchet',
    rc_Otchestvo VARCHAR COMMENT 'rc_Otchestvo',
    rc_PodrazdeleniePoOrgShtatnoyStrukture VARCHAR COMMENT 'rc_PodrazdeleniePoOrgShtatnoyStrukture',
    rc_PodrazdeleniePoUpravlencheskoyStrukture VARCHAR COMMENT 'rc_PodrazdeleniePoUpravlencheskoyStrukture',
    rc_Pol VARCHAR COMMENT 'rc_Pol',
    rc_PriznakMeditsinskoyOrganizatsii BOOLEAN COMMENT 'rc_PriznakMeditsinskoyOrganizatsii',
    rc_Samozanyatyy BOOLEAN COMMENT 'rc_Samozanyatyy',
    rc_SvidetelstvoDataVydachi VARCHAR COMMENT 'rc_SvidetelstvoDataVydachi',
    rc_SvidetelstvoSeriyaNomer VARCHAR COMMENT 'rc_SvidetelstvoSeriyaNomer',
    rc_SNILS VARCHAR COMMENT 'rc_SNILS',
    rc_SubektMalogoIliSrednegoPredprinimatelstvaUKh BOOLEAN COMMENT 'rc_SubektMalogoIliSrednegoPredprinimatelstvaUKh',
    rc_Familiya VARCHAR COMMENT 'rc_Familiya',
    rc_FIOIDolzhnostRukovoditelya VARCHAR COMMENT 'rc_FIOIDolzhnostRukovoditelya',
    rc_FIOIDolzhnostRukovoditelyaVRoditelnomPadezhe VARCHAR COMMENT 'rc_FIOIDolzhnostRukovoditelyaVRoditelnomPadezhe',
    rc_FormaUchastiyaVKapitale BOOLEAN COMMENT 'rc_FormaUchastiyaVKapitale',
    rc_EtoBank BOOLEAN COMMENT 'rc_EtoBank',
    rc_EtoStrakhovayaKompaniya BOOLEAN COMMENT 'rc_EtoStrakhovayaKompaniya',
    rc_YuridicheskoeFizicheskoeLitso VARCHAR COMMENT 'rc_YuridicheskoeFizicheskoeLitso',
    GolovnoyKontragent VARCHAR COMMENT 'GolovnoyKontragent',
    DokumentUdostoveryayushchiyLichnost VARCHAR COMMENT 'DokumentUdostoveryayushchiyLichnost',
    Identifikator VARCHAR COMMENT 'Identifikator',
    INN VARCHAR COMMENT 'INN',
    KodOKOPF VARCHAR COMMENT 'KodOKOPF',
    KodPoOKPO VARCHAR COMMENT 'KodPoOKPO',
    Kommentariy VARCHAR COMMENT 'Kommentariy',
    KPP VARCHAR COMMENT 'KPP',
    MezhdunarodnoeNaimenovanie VARCHAR COMMENT 'MezhdunarodnoeNaimenovanie',
    NaimenovanieOKOPF VARCHAR COMMENT 'NaimenovanieOKOPF',
    NalogovyyNomer VARCHAR COMMENT 'NalogovyyNomer',
    Nerezident BOOLEAN COMMENT 'Nerezident',
    ObosoblennoePodrazdelenie BOOLEAN COMMENT 'ObosoblennoePodrazdelenie',
    OGRN VARCHAR COMMENT 'OGRN',
    OKDP VARCHAR COMMENT 'OKDP',
    Opisanie VARCHAR COMMENT 'Opisanie',
    OrganizatsionnayaEdinitsa VARCHAR COMMENT 'OrganizatsionnayaEdinitsa',
    OrganizatsionnoPravovayaForma VARCHAR COMMENT 'OrganizatsionnoPravovayaForma',
    Partner VARCHAR COMMENT 'Partner',
    PolnoeNaimenovanie VARCHAR COMMENT 'PolnoeNaimenovanie',
    PriznakIspolzovaniya VARCHAR COMMENT 'PriznakIspolzovaniya',
    RegistratsionnyyNomerNerezidenta VARCHAR COMMENT 'RegistratsionnyyNomerNerezidenta',
    SokrashchennoeNaimenovanie VARCHAR COMMENT 'SokrashchennoeNaimenovanie',
    StranaRegistratsii VARCHAR COMMENT 'StranaRegistratsii',
    UdalitZapisNeNormalizuema BOOLEAN COMMENT 'UdalitZapisNeNormalizuema',
    UdalitTipPozitsii VARCHAR COMMENT 'UdalitTipPozitsii',
    UdalitEtalonnayaPozitsiya VARCHAR COMMENT 'UdalitEtalonnayaPozitsiya',
    UdalitEtoMaket BOOLEAN COMMENT 'UdalitEtoMaket',
    YurFizLitso VARCHAR COMMENT 'YurFizLitso',
    OsnovnoyBankovskiySchet VARCHAR COMMENT 'OsnovnoyBankovskiySchet',
    MomentOfTime DATETIME COMMENT 'MomentOfTime',
    load_timestamp DATETIME COMMENT 'Время загрузки'
);

-- Таблица stg_contractor_debt для задолженности контрагентов из топика sys__asb__esud_rzdm__contractor_debt__data
CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_contractor_debt (
    Organization VARCHAR COMMENT 'Организация',
    OrganizationINN VARCHAR COMMENT 'ИНН организации',
    OrganizationKPP VARCHAR COMMENT 'КПП организации',
    OrganizationMDMKey VARCHAR COMMENT 'MDM ключ организации',
    Counterparty VARCHAR COMMENT 'Контрагент',
    CounterpartyINN VARCHAR COMMENT 'ИНН контрагента',
    CounterpartyKPP VARCHAR COMMENT 'КПП контрагента',
    CounterpartyMDMKey VARCHAR COMMENT 'MDM ключ контрагента',
    Contract VARCHAR COMMENT 'Договор',
    AmountOwed DOUBLE COMMENT 'Сумма задолженности',
    TimeStamp DATETIME COMMENT 'Временная метка',
    load_timestamp DATETIME COMMENT 'Время загрузки'
);

-- Таблица stg_employees для сотрудников из топика sys__kuirzp__esud_rzdm__employee__data
CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_employees (
    Organization VARCHAR COMMENT 'Организация',
    OrganizationINN VARCHAR COMMENT 'ИНН организации',
    OrganizationKPP VARCHAR COMMENT 'КПП организации',
    OrganizationMDMKey VARCHAR COMMENT 'MDM ключ организации',
    Division VARCHAR COMMENT 'Подразделение',
    DivisionMDMKey VARCHAR COMMENT 'MDM ключ подразделения',
    Employee VARCHAR COMMENT 'Сотрудник (GUID)',
    TableNumber VARCHAR COMMENT 'Табельный номер',
    Post VARCHAR COMMENT 'Должность',
    Gender VARCHAR COMMENT 'Пол',
    TypeOfEmployment VARCHAR COMMENT 'Вид занятости',
    TimeStamp DATETIME COMMENT 'Временная метка',
    load_timestamp DATETIME COMMENT 'Время загрузки'
);

-- Таблица stg_orders для заказов из топика sys__isras__esud_rzdm__order__data
CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_orders (
    order_id VARCHAR COMMENT 'Идентификатор заказа',
    order_date DATETIME COMMENT 'Дата заказа',
    org_inn VARCHAR COMMENT 'ИНН организации',
    org_kpp VARCHAR COMMENT 'КПП организации',
    org_name VARCHAR COMMENT 'Наименование организации',
    org_mdmkey VARCHAR COMMENT 'MDM ключ организации',
    depart_code VARCHAR COMMENT 'Код подразделения',
    depart_name VARCHAR COMMENT 'Наименование подразделения',
    depart_mdmkey VARCHAR COMMENT 'MDM ключ подразделения',
    patient_code VARCHAR COMMENT 'Код пациента',
    order_cod VARCHAR COMMENT 'Код заказа',
    doctor_code VARCHAR COMMENT 'Код врача',
    doctor_name VARCHAR COMMENT 'ФИО врача',
    tostore_mdmkey VARCHAR COMMENT 'MDM ключ склада назначения',
    tostore_name VARCHAR COMMENT 'Наименование склада назначения',
    item_type VARCHAR COMMENT 'Тип позиции (goods/drugs)',
    id_cpz VARCHAR COMMENT 'Идентификатор ЦПЗ',
    goods_name VARCHAR COMMENT 'Наименование товара',
    goods_qnty INT COMMENT 'Количество товара',
    drug_mnn VARCHAR COMMENT 'МНН препарата',
    drug_doze VARCHAR COMMENT 'Дозировка препарата',
    drug_form VARCHAR COMMENT 'Форма препарата',
    drug_qnty VARCHAR COMMENT 'Количество препарата',
    load_timestamp DATETIME COMMENT 'Время загрузки'
);

-- Таблица stg_movements для движений товаров из топика sys__isras__esud_rzdm__movement__data
CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_movements (
    transfer_isras_id VARCHAR COMMENT 'Идентификатор перемещения ИСРАС',
    transfer_mis_id VARCHAR COMMENT 'Идентификатор перемещения МИС',
    org_inn VARCHAR COMMENT 'ИНН организации',
    org_kpp VARCHAR COMMENT 'КПП организации',
    org_name VARCHAR COMMENT 'Наименование организации',
    org_mdmkey VARCHAR COMMENT 'MDM ключ организации',
    transfer_num VARCHAR COMMENT 'Номер перемещения',
    transfer_date DATETIME COMMENT 'Дата перемещения',
    fromstore_mdmkey VARCHAR COMMENT 'MDM ключ склада-источника',
    fromstore_name VARCHAR COMMENT 'Наименование склада-источника',
    tostore_mdmkey VARCHAR COMMENT 'MDM ключ склада назначения',
    tostore_name VARCHAR COMMENT 'Наименование склада назначения',
    transfer_finsource_mdmkey VARCHAR COMMENT 'MDM ключ источника финансирования',
    transfer_finsource_name VARCHAR COMMENT 'Наименование источника финансирования',
    transfer_sourcedoc_num VARCHAR COMMENT 'Номер исходного документа',
    transfer_sourcedoc_date DATETIME COMMENT 'Дата исходного документа',
    id_cpz VARCHAR COMMENT 'Идентификатор ЦПЗ',
    goods_name VARCHAR COMMENT 'Наименование товара',
    goods_qnty INT COMMENT 'Количество товара',
    goods_sn VARCHAR COMMENT 'Серийный номер товара',
    goods_expdate DATETIME COMMENT 'Срок годности товара',
    goods_price DOUBLE COMMENT 'Цена товара',
    goods_mesunit VARCHAR COMMENT 'Единица измерения',
    goods_barcode VARCHAR COMMENT 'Штрих-код товара',
    goods_sum DOUBLE COMMENT 'Сумма товара',
    goods_sumnds DOUBLE COMMENT 'Сумма НДС товара',
    goods_ndspercent VARCHAR COMMENT 'Процент НДС',
    goods_producerprice DOUBLE COMMENT 'Цена производителя',
    load_timestamp DATETIME COMMENT 'Время загрузки'
);

-- Таблица stg_forms для форм отчетности из топика sys__buinu__esud_rzdm__form__data
CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_forms (
    orgname VARCHAR COMMENT 'Наименование организации',
    orginn VARCHAR COMMENT 'ИНН организации',
    orgkpp VARCHAR COMMENT 'КПП организации',
    kodform VARCHAR COMMENT 'Код формы',
    period1 DATETIME COMMENT 'Начало периода',
    period2 DATETIME COMMENT 'Конец периода',
    kodstr VARCHAR COMMENT 'Код строки',
    kodkol INT COMMENT 'Код колонки',
    summa DOUBLE COMMENT 'Сумма',
    load_timestamp DATETIME COMMENT 'Время загрузки'
);

-- Таблица stg_buildings для зданий из топика sys__mis__esud_rzdm._data_99099.fdb.BUILDINGS
CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_buildings (
    `values` VARCHAR COMMENT 'JSON данные (не распарсенные)',
    load_timestamp DATETIME COMMENT 'Время загрузки'
);

-- Таблица stg_chairs для кресел из топика sys__mis__esud_rzdm._data_99099.fdb.CHAIRS
CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_chairs (
    `values` VARCHAR COMMENT 'JSON данные (не распарсенные)',
    load_timestamp DATETIME COMMENT 'Время загрузки'
);

-- Таблица stg_rooms для комнат из топика sys__mis__esud_rzdm._data_99099.fdb.ROOMS
CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_rooms (
    `values` VARCHAR COMMENT 'JSON данные (не распарсенные)',
    load_timestamp DATETIME COMMENT 'Время загрузки'
);

-- Таблица stg_kpi для KPI из топика sys__kuirzp__esud_rzdm__kpi__data
CREATE TABLE IF NOT EXISTS iceberg.rzdm.stg_kpi (
    `values` VARCHAR COMMENT 'JSON данные (не распарсенные)',
    load_timestamp DATETIME COMMENT 'Время загрузки'
);
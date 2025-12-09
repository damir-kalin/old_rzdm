from datetime import datetime, timedelta
import os
import re

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator

from starrocks_operators import (
    StarRocksDropTableOperator,
    StarRocksDropViewOperator,
)
from vk_cloud_operators import VKCloudS3Sensor, Template2Operator

# Bucket to schema mapping
BUCKET_TO_SCHEMA = {
    'indefinite': 'stg_indefinite',
    'infoclinic': 'stg_mis',
    'asb': 'stg_asb',
    'asckz': 'stg_asckz',
    'kuirzp': 'stg_kuirzp',
    'nsi': 'stg_nsi',
    'buinu': 'stg_buinu',
}

# Bucket to S3 connection mapping
BUCKET_TO_CONNECTION = {
    'indefinite': 'rzdm_indefinite_s3',
    'infoclinic': 'rzdm_infoclinic_s3',
    'asb': 'rzdm_asb_s3',
    'asckz': 'rzdm_asckz_s3',
    'kuirzp': 'rzdm_kuirzp_s3',
    'nsi': 'rzdm_nsi_s3',
    'buinu': 'rzdm_buinu_s3',
}

# Detect environment and set bucket names for Template 2
ENV = os.getenv('AIRFLOW_ENV', 'dev')
if ENV == 'prod':
    BUCKET_NAMES = [
        "rzdm-prod-indefinite-sys-bucket",
        "rzdm-prod-infoclinic-sys-bucket",
        "rzdm-prod-asb-sys-bucket",
        "rzdm-prod-asckz-sys-bucket",
        "rzdm-prod-kuirzp-sys-bucket",
        "rzdm-prod-nsi-sys-bucket",
        "rzdm-prod-buinu-sys-bucket"
    ]
elif ENV == 'test':
    BUCKET_NAMES = [
        "rzdm-test-indefinite-sys-bucket",
        "rzdm-test-infoclinic-sys-bucket",
        "rzdm-test-asb-sys-bucket",
        "rzdm-test-asckz-sys-bucket",
        "rzdm-test-kuirzp-sys-bucket",
        "rzdm-test-nsi-sys-bucket",
        "rzdm-test-buinu-sys-bucket"
    ]
else:  # dev
    BUCKET_NAMES = [
        "rzdm-dev-indefinite-sys-bucket",
        "rzdm-dev-infoclinic-sys-bucket",
        "rzdm-dev-asb-sys-bucket",
        "rzdm-dev-asckz-sys-bucket",
        "rzdm-dev-kuirzp-sys-bucket",
        "rzdm-dev-nsi-sys-bucket",
        "rzdm-dev-buinu-sys-bucket"
    ]


# Create bucket-to-connection mapping for sensor
SENSOR_BUCKET_CONNECTIONS = {
    f"rzdm-{ENV}-indefinite-sys-bucket": "rzdm_indefinite_s3",
    f"rzdm-{ENV}-infoclinic-sys-bucket": "rzdm_infoclinic_s3",
    f"rzdm-{ENV}-asb-sys-bucket": "rzdm_asb_s3",
    f"rzdm-{ENV}-asckz-sys-bucket": "rzdm_asckz_s3",
    f"rzdm-{ENV}-kuirzp-sys-bucket": "rzdm_kuirzp_s3",
    f"rzdm-{ENV}-nsi-sys-bucket": "rzdm_nsi_s3",
    f"rzdm-{ENV}-buinu-sys-bucket": "rzdm_buinu_s3",
}


def is_template2_file(file_name: str) -> bool:
    """
    Template 2 = any Excel file that is NOT Форма 1/2
    Examples: Аптеки.xlsx, страховка бизнес.xls, База_выручки.xlsx
    """
    # Exclude financial forms (Template 1)
    forms_patterns = [
        r'форма\s*[12]',
        r'form\s*[12]'
    ]
    for pattern in forms_patterns:
        if re.search(pattern, file_name, re.IGNORECASE):
            return False

    # Accept any Excel file
    return file_name.lower().endswith(('.xlsx', '.xls', '.xlsm'))


def get_changed_files(**context):
    """Get list of changed files for dynamic processing"""
    changed_files = context['ti'].xcom_pull(
        task_ids='vk_cloud_s3_sensor', key='changed_files'
    )

    if not changed_files:
        return []

    # Filter to Template 2 files only
    template2_files = [f for f in changed_files if is_template2_file(f['file_name'])]

    # Limit to first 10 files to avoid XCom size limit
    if len(template2_files) > 10:
        print(f"WARNING: {len(template2_files)} Template 2 files detected, limiting to first 10")
        return template2_files[:10]

    return template2_files


def get_missing_files(**context):
    """Get list of missing files for cleanup"""
    missing_files = context['ti'].xcom_pull(
        task_ids='vk_cloud_s3_sensor', key='missing_files'
    )

    if not missing_files:
        return []

    # Filter to Template 2 files only
    template2_files = [f for f in missing_files if is_template2_file(f['file_name'])]

    # Limit to first 10 files
    if len(template2_files) > 10:
        print(f"WARNING: {len(template2_files)} missing Template 2 files, limiting to first 10")
        return template2_files[:10]

    return template2_files


def get_schema_from_bucket(bucket_name: str) -> str:
    """
    Map bucket name to StarRocks schema
    Examples:
      rzdm-dev-indefinite-sys-bucket -> stg_indefinite
      rzdm-test-infoclinic-sys-bucket -> stg_mis
    """
    for key, schema in BUCKET_TO_SCHEMA.items():
        if key in bucket_name:
            return schema
    return 'stage'


def get_connection_from_bucket(bucket_name: str) -> str:
    """
    Map bucket name to S3 connection ID
    Examples:
      rzdm-dev-indefinite-sys-bucket -> rzdm_indefinite_s3
      rzdm-test-asb-sys-bucket -> rzdm_asb_s3
    """
    for key, conn_id in BUCKET_TO_CONNECTION.items():
        if key in bucket_name:
            return conn_id
    return 'rzdm_lake_s3'


def build_table_name(s3_timestamp: str, filename: str) -> str:
    """
    Build table name from S3 timestamp and filename
    Keep Cyrillic characters, remove _--_user_--_id suffix
    Examples:
      2025-12-07T08:08:00Z, Шаблон 2 - КПЭ_--_user_--_id.xlsx -> pattern_2_data_load_2025-12-07_08_08_00_Шаблон_2_КПЭ_xlsx
      2025-11-15T14:30:45Z, База выручки.xls -> pattern_2_data_load_2025-11-15_14_30_45_База_выручки_xls
    """
    # Parse timestamp and format as YYYY-MM-DD_HH_MM_SS
    if 'T' in s3_timestamp:
        timestamp_part = s3_timestamp.split('T')[0] + '_' + s3_timestamp.split('T')[1][:8].replace(':', '_')
    else:
        timestamp_part = s3_timestamp.replace(':', '_').replace(' ', '_')

    # Remove _--_user_--_id suffix from filename
    cleaned_name = re.sub(r'_--_user_--_id.*$', '', filename)

    # Remove extension
    name_no_ext = cleaned_name.rsplit('.', 1)[0] if '.' in cleaned_name else cleaned_name

    # Get extension
    ext = cleaned_name.rsplit('.', 1)[1] if '.' in cleaned_name else 'xlsx'

    # Replace spaces and dashes with underscores, keep Cyrillic
    sanitized = name_no_ext.replace(' ', '_').replace('-', '_')

    # Remove duplicate underscores
    while '__' in sanitized:
        sanitized = sanitized.replace('__', '_')

    return f"pattern_2_data_load_{timestamp_part}_{sanitized}_{ext}"


def process_changed_file(**context):
    """
    Process one Template 2 (KPI) file.
    Uses VK Cloud S3 and Template2Operator.
    """
    file_data = context['file_data']

    start_time = datetime.now()
    file_name = file_data['file_name']
    bucket_name = file_data['bucket']
    key = file_data['key']
    field = file_data['guid']
    status = file_data.get('status', 'new')
    file_size = file_data.get('size', 0)
    s3_timestamp = file_data.get('last_modified', datetime.now().isoformat())

    # Skip if not Template 2
    if not is_template2_file(file_name):
        print(f"Skipping {file_name} - not a Template 2 file (probably Форма 1/2)")
        return {
            'file_name': file_name,
            'status': 'skipped',
            'reason': 'not_template2',
            'bucket': bucket_name,
        }

    # Get bucket-specific schema and build table name
    destination_schema = get_schema_from_bucket(bucket_name)
    starrocks_table = build_table_name(s3_timestamp, file_name)

    # Get bucket-specific S3 connection
    s3_conn_id = get_connection_from_bucket(bucket_name)

    steps_completed = []
    rows_loaded = 0

    try:
        # Drop existing table if file changed
        if status == "changed":
            print(f"File {file_name} changed, dropping table {destination_schema}.{starrocks_table}")
            drop_operator = StarRocksDropTableOperator(
                task_id=f"starrocks_drop_{field}",
                starrocks_database=destination_schema,
                starrocks_table=starrocks_table,
                starrocks_conn_id="starrocks_default",
                dag=None,
            )
            drop_operator.execute({})
            steps_completed.append('drop_table')

        # Process Template 2 file using Template2Operator
        print(f"Processing Template 2 file: {file_name}")
        print(f"Schema: {destination_schema}, Table: {starrocks_table}")
        template2_operator = Template2Operator(
            task_id=f"template2_{field}",
            s3_bucket=bucket_name,
            s3_key=key,
            file_id=field,
            starrocks_table=starrocks_table,
            file_name=file_name,
            user_name=file_data.get('user_name', 'vk_cloud'),
            last_modified=s3_timestamp,
            s3_conn_id=s3_conn_id,
            starrocks_conn_id="starrocks_default",
            starrocks_database=destination_schema,
            postgres_conn_id="airflow_db",
            enable_logging=True,
        )

        result = template2_operator.execute({})

        if result['success']:
            rows_loaded = result.get('rows_loaded', result.get('rows_processed', 0))
            steps_completed.append('template2_processing')
            steps_completed.append('load_to_starrocks')
        else:
            raise Exception(f"Template 2 processing failed: {result.get('error')}")

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        return {
            'file_name': file_name,
            'status': 'success',
            'bucket': bucket_name,
            'destination_schema': destination_schema,
            'destination_table': starrocks_table,
            'starrocks_table': starrocks_table,
            'file_status': status,
            'file_size_bytes': file_size,
            'rows_loaded': rows_loaded,
            'steps_completed': steps_completed,
            'processing_time_seconds': round(processing_time, 2),
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
        }

    except Exception as e:
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        return {
            'file_name': file_name,
            'status': 'error',
            'error': str(e),
            'bucket': bucket_name,
            'file_status': status,
            'file_size_bytes': file_size,
            'steps_completed': steps_completed,
            'processing_time_seconds': round(processing_time, 2),
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
        }


def process_missing_file(**context):
    """
    Process deleted file - cleanup tables in StarRocks.
    """
    file_data = context['file_data']

    start_time = datetime.now()
    file_name = file_data['file_name']
    bucket_name = file_data['bucket']
    field = file_data['guid']
    status = file_data['status']
    file_size = file_data.get('size', 0)
    s3_timestamp = file_data.get('last_modified', datetime.now().isoformat())

    # Get bucket-specific schema and build table name
    destination_schema = get_schema_from_bucket(bucket_name)
    starrocks_table = build_table_name(s3_timestamp, file_name)

    steps_completed = []
    errors = []

    try:
        # Drop table in StarRocks
        print(f"Dropping table in StarRocks: {destination_schema}.{starrocks_table}")
        try:
            drop_operator = StarRocksDropTableOperator(
                task_id=f"starrocks_drop_{field}",
                starrocks_database=destination_schema,
                starrocks_table=starrocks_table,
                starrocks_conn_id="starrocks_default",
                dag=None,
            )
            drop_result = drop_operator.execute({})
            if drop_result:
                steps_completed.append('drop_table')
        except Exception as e:
            steps_completed.append('drop_table_error')
            errors.append(f"drop_table: {str(e)}")

        # Drop view in StarRocks
        print(f"Dropping view in StarRocks: {destination_schema}.v_{starrocks_table}")
        try:
            drop_view_operator = StarRocksDropViewOperator(
                task_id=f"starrocks_drop_view_{field}",
                starrocks_database=destination_schema,
                starrocks_view="v_" + starrocks_table,
                starrocks_conn_id="starrocks_default",
                dag=None,
            )
            drop_view_result = drop_view_operator.execute({})
            if drop_view_result:
                steps_completed.append('drop_view')
        except Exception as e:
            steps_completed.append('drop_view_error')
            errors.append(f"drop_view: {str(e)}")

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        return {
            'file_name': file_name,
            'status': 'success',
            'bucket': bucket_name,
            'destination_schema': destination_schema,
            'destination_table': starrocks_table,
            'starrocks_table': starrocks_table,
            'file_status': status,
            'file_size_bytes': file_size,
            'steps_completed': steps_completed,
            'errors': errors,
            'processing_time_seconds': round(processing_time, 2),
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
        }

    except Exception as e:
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        return {
            'file_name': file_name,
            'status': 'error',
            'error': str(e),
            'bucket': bucket_name,
            'file_status': status,
            'file_size_bytes': file_size,
            'steps_completed': steps_completed,
            'errors': errors,
            'processing_time_seconds': round(processing_time, 2),
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
        }


def summarize_results(**context):
    """
    Summarize processing results for all files.
    """
    changed_results = context.get('changed_results')
    missing_results = context.get('missing_results')

    # Normalize results
    parts = []
    for part in [changed_results, missing_results]:
        if part is None:
            continue
        try:
            seq = list(part)
        except Exception:
            seq = [part]
        for item in seq:
            if isinstance(item, (list, tuple)):
                parts.extend([x for x in item if isinstance(x, dict)])
            elif isinstance(item, dict):
                parts.append(item)

    results = parts

    if not results:
        print("No files to process. Both branches empty (new/changed/missing = 0)")
        return {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'total_rows_loaded': 0,
            'total_processing_time_seconds': 0,
            'total_size_bytes': 0,
        }

    successful = [r for r in results if r.get('status') == 'success']
    failed = [r for r in results if r.get('status') == 'error']
    skipped = [r for r in results if r.get('status') == 'skipped']

    total_rows = sum(r.get('rows_loaded', 0) for r in successful)
    total_time = sum(r.get('processing_time_seconds', 0) for r in results)
    total_size = sum(r.get('file_size_bytes', 0) for r in results)

    print("=" * 80)
    print("TEMPLATE 2 (KPI) PROCESSING SUMMARY")
    print("=" * 80)
    print(f"Total files: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print(f"Skipped: {len(skipped)}")
    print(f"Total processing time: {total_time:.2f} sec")
    print(f"Total file size: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)")

    if successful:
        print("\n" + "=" * 80)
        print(f"SUCCESSFULLY PROCESSED FILES ({len(successful)})")
        print("=" * 80)
        for idx, result in enumerate(successful, 1):
            print(f"\n{idx}. File: {result['file_name']}")
            print(f"   Bucket: {result['bucket']}")
            print(f"   Table: {result['destination_table']}")
            print(f"   Rows loaded: {result.get('rows_loaded', 0):,}")
            print(f"   Processing time: {result['processing_time_seconds']:.2f} sec")

    if skipped:
        print("\n" + "=" * 80)
        print(f"SKIPPED FILES ({len(skipped)})")
        print("=" * 80)
        for idx, result in enumerate(skipped, 1):
            print(f"\n{idx}. File: {result['file_name']}")
            print(f"   Reason: {result.get('reason', 'unknown')}")

    if failed:
        print("\n" + "=" * 80)
        print(f"FAILED FILES ({len(failed)})")
        print("=" * 80)
        for idx, result in enumerate(failed, 1):
            print(f"\n{idx}. File: {result['file_name']}")
            print(f"   Error: {result.get('error', 'Unknown error')}")

    print("\n" + "=" * 80)

    return {
        'total': len(results),
        'successful': len(successful),
        'failed': len(failed),
        'skipped': len(skipped),
        'total_rows_loaded': total_rows,
        'total_processing_time_seconds': round(total_time, 2),
        'total_size_bytes': total_size,
    }


# DAG DEFINITION
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    "template2_load_to_starrocks",
    default_args=DEFAULT_ARGS,
    schedule=None,  # Manual trigger
    catchup=False,
    tags=["template2", "kpi", "starrocks", "excel"],
    max_active_runs=1,
    description="DAG for loading Template 2 (KPI) Excel files to StarRocks",
) as dag:

    # Sensor for monitoring files in VK Cloud S3 across all 7 buckets
    vk_cloud_sensor = VKCloudS3Sensor(
        task_id="vk_cloud_s3_sensor",
        s3_buckets=BUCKET_NAMES,  # ALL 7 buckets
        s3_bucket_connections=SENSOR_BUCKET_CONNECTIONS,  # Bucket-specific connections
        s3_prefix="manual-data/",
        file_pattern=r'\.xls[xmb]?$',  # Any Excel file
        postgres_conn_id="airflow_db",
        enable_logging=True,
        poke_interval=15,
        timeout=3600,
        mode="reschedule",
    )

    # Get changed files
    get_changed_files_task = PythonOperator(
        task_id="get_changed_files",
        python_callable=get_changed_files,
    )

    # Get missing files
    get_missing_files_task = PythonOperator(
        task_id="get_missing_files",
        python_callable=get_missing_files,
    )

    # Process changed files (dynamic task mapping)
    process_changed_files = PythonOperator.partial(
        task_id="process_changed_file",
        python_callable=process_changed_file,
        max_active_tis_per_dagrun=2,
    ).expand(op_kwargs=get_changed_files_task.output.map(lambda f: {'file_data': f}))

    # Process missing files (dynamic task mapping)
    process_missing_files = PythonOperator.partial(
        task_id="process_missing_file",
        python_callable=process_missing_file,
        max_active_tis_per_dagrun=2,
    ).expand(op_kwargs=get_missing_files_task.output.map(lambda f: {'file_data': f}))

    # Summarize results
    summarize_task = PythonOperator(
        task_id="summarize_results",
        python_callable=summarize_results,
        op_kwargs={
            'changed_results': process_changed_files.output,
            'missing_results': process_missing_files.output,
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Dependencies
    vk_cloud_sensor >> get_changed_files_task
    vk_cloud_sensor >> get_missing_files_task
    get_changed_files_task >> process_changed_files
    get_missing_files_task >> process_missing_files
    [process_changed_files, process_missing_files] >> summarize_task

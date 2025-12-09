from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task

from starrocks_operators import (
    StarRocksDropTableOperator,
    StarRocksDropViewOperator,
)
from vk_cloud_operators import VKCloudS3Sensor, FinancialFormsOperator
import re

# Detect environment and set bucket name
ENV = os.getenv('AIRFLOW_ENV', 'dev')
if ENV == 'prod':
    BUCKET_NAME = "rzdm-prod-buinu-sys-bucket"
elif ENV == 'test':
    BUCKET_NAME = "rzdm-test-buinu-sys-bucket"
else:
    BUCKET_NAME = "rzdm-dev-buinu-sys-bucket"
BUCKET_NAME_CLEAN = BUCKET_NAME.replace("-", "_")


def is_financial_form(file_name: str) -> bool:
    """Detect if file is a financial form (Form 1 or Form 2, any year, any period)"""
    patterns = [
        r'форма\s*[12].*\d{4}.*\.xls[xmb]?$',
        r'form\s*[12].*\d{4}.*\.xls[xmb]?$',
    ]
    file_name_lower = file_name.lower()
    return any(re.search(pattern, file_name_lower) for pattern in patterns)


def get_changed_files(**context):
    """Получить список файлов для динамической обработки"""
    changed_files = context['ti'].xcom_pull(
        task_ids='vk_cloud_s3_sensor', key='changed_files'
    )

    if not changed_files:
        return []

    # Limit to first 10 files to avoid XCom size limit
    if len(changed_files) > 10:
        print(f"WARNING: {len(changed_files)} changed files detected, limiting to first 10")
        return changed_files[:10]

    return changed_files


def get_missing_files(**context):
    """Получить список файлов для динамической обработки"""
    missing_files = context['ti'].xcom_pull(
        task_ids='vk_cloud_s3_sensor', key='missing_files'
    )

    if not missing_files:
        return []

    # Limit to first 10 files to avoid XCom size limit
    if len(missing_files) > 10:
        print(f"WARNING: {len(missing_files)} missing files detected, limiting to first 10")
        return missing_files[:10]

    return missing_files


def process_changed_file(**context):
    """
    Обработать один файл финансовой формы.
    Использует VK Cloud S3 и VKStreamLoader.
    """
    file_data = context['file_data']

    start_time = datetime.now()
    file_name = file_data['file_name']
    bucket_name = file_data['bucket']
    key = file_data['key']
    field = file_data['guid']
    table_name = file_data['table_name']
    destination_schema = "stg_buinu"
    status = file_data.get('status', 'new')
    file_size = file_data.get('size', 0)
    base_starrocks_table = f"{table_name}_{bucket_name}".replace("-", "_")

    # VK Cloud S3 connection
    s3_conn_id = "rzdm_lake_s3"

    is_form = is_financial_form(file_name)

    steps_completed = []
    rows_loaded = 0

    try:
        # NOTE: No longer dropping tables - form_1_assets and form_2_financial are static tables
        # Data is appended to existing tables

        # Process financial form using FinancialFormsOperator
        print(f"Processing Financial Form: {file_name}")
        forms_operator = FinancialFormsOperator(
            task_id=f"financial_form_{field}",
            s3_bucket=bucket_name,
            s3_key=key,
            file_id=field,
            s3_conn_id=s3_conn_id,
            starrocks_conn_id="starrocks_default",
            starrocks_database=destination_schema,
            postgres_conn_id="airflow_db",
            enable_logging=True,  # Enable file status tracking in forms_file_metadata
        )

        form_result = forms_operator.execute({})

        if form_result['success']:
            rows_loaded = form_result.get('rows_loaded', form_result.get('rows_processed', 0))
            steps_completed.append('financial_form_processing')
            steps_completed.append('load_to_starrocks')
        else:
            raise Exception(f"Financial form processing failed: {form_result.get('error')}")

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        starrocks_table = form_result.get('table_name', 'unknown')

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
    Обработать удаленный файл - очистка таблиц в StarRocks.
    НЕ пишет в метастор.
    """
    file_data = context['file_data']

    start_time = datetime.now()
    file_name = file_data['file_name']
    bucket_name = file_data['bucket']
    field = file_data['guid']
    table_name = file_data['table_name']
    starrocks_table = f"{table_name}_{bucket_name}".replace("-", "_")
    destination_schema = "stg_buinu"
    status = file_data['status']
    file_size = file_data.get('size', 0)

    steps_completed = []
    errors = []

    try:
        # Drop table in StarRocks
        print(f"Удаление таблицы в StarRocks: {destination_schema}.{table_name}")
        try:
            drop_operator = StarRocksDropTableOperator(
                task_id=f"starrocks_drop_{field}",
                starrocks_database=destination_schema,
                starrocks_table=table_name,
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
        print(f"Удаление представления в StarRocks: {destination_schema}.v_{table_name}")
        try:
            drop_view_operator = StarRocksDropViewOperator(
                task_id=f"starrocks_drop_view_{field}",
                starrocks_database=destination_schema,
                starrocks_view="v_" + table_name,
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
            'destination_table': table_name,
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
    Подвести итоги обработки всех файлов.
    Выводит детальную информацию о каждом обработанном файле.
    """
    changed_results = context.get('changed_results')
    missing_results = context.get('missing_results')

    # Нормализуем входные результаты из двух веток
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
        print(
            "Нет файлов для обработки. Обе ветки пустые "
            "(new/changed/missing = 0)"
        )
        return {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'total_rows_loaded': 0,
            'total_processing_time_seconds': 0,
            'total_size_bytes': 0,
        }

    # results уже нормализован

    successful = [r for r in results if r.get('status') == 'success']
    failed = [r for r in results if r.get('status') == 'error']
    deleted_files = [r for r in results if r.get('status') == 'deleted']

    # Разбивка по типу файла (new/changed/missing)
    by_kind = {
        'new': [r for r in successful if r.get('file_status') == 'new'],
        'changed': [r for r in successful if r.get('file_status') == 'changed'],
        'missing': [r for r in results if r.get('file_status') == 'missing'],
    }

    total_rows = sum(r.get('rows_loaded', 0) for r in successful)
    total_time = sum(r.get('processing_time_seconds', 0) for r in results)
    total_size = sum(r.get('file_size_bytes', 0) for r in results)

    print("=" * 80)
    print("ИТОГОВАЯ СТАТИСТИКА ОБРАБОТКИ ФАЙЛОВ")
    print("=" * 80)
    print(f"Всего файлов обработано: {len(results)}")
    print(f"Успешно: {len(successful)}")
    print(f"С ошибками: {len(failed)}")
    print(f"Общее время обработки: {total_time:.2f} сек")
    print(f"Общий размер файлов: {total_size:,} байт "
          f"({total_size / 1024 / 1024:.2f} MB)")

    # Краткая сводка по типам
    print("КАТЕГОРИИ:")
    print(f"  new: {len(by_kind['new'])}")
    print(f"  changed: {len(by_kind['changed'])}")
    print(f"  missing: {len(by_kind['missing'])}")

    if successful:
        print("\n" + "=" * 80)
        print(f"УСПЕШНО ОБРАБОТАННЫЕ ФАЙЛЫ ({len(successful)})")
        print("=" * 80)

        for idx, result in enumerate(successful, 1):
            print(f"\n{idx}. Файл: {result['file_name']}")
            print(f"   Бакет: {result['bucket']}")
            print(f"   Статус файла: {result.get('file_status', '-')}")
            print(f"   Схема назначения: {result['destination_schema']}")
            print(f"   Таблица: {result['destination_table']}")
            print(f"   Размер файла: {result['file_size_bytes']:,} байт")
            print(
                f"   Загружено строк: {result.get('rows_loaded', 0):,}"
            )
            print(f"   Таблица в destination_schema удалена: "
                  f"{'Да' if result.get('table_dropped') else 'Нет'}")
            print(f"   Старый файл удален: "
                  f"{'Да' if result.get('old_file_deleted') else 'Нет'}")
            print(f"   Осуществлено удаление таблицы в stage:"
                  f"{'Да' if result.get('dropped_stage') else 'Нет'}")
            print(f"   Время обработки: "
                  f"{result['processing_time_seconds']:.2f} сек")
            print(f"   Начало: {result['start_time']}")
            print(f"   Конец: {result['end_time']}")

            steps = result.get('steps_completed', [])
            if steps:
                print(f"   Выполненные шаги ({len(steps)}): "
                      f"{', '.join(steps)}")

    if failed:
        print("\n" + "=" * 80)
        print(f"ФАЙЛЫ С ОШИБКАМИ ({len(failed)})")
        print("=" * 80)

        for idx, result in enumerate(failed, 1):
            print(f"\n{idx}. Файл: {result['file_name']}")
            print(f"   Бакет: {result['bucket']}")
            print(f"   Статус файла: {result.get('file_status', '-')}")
            print(f"   Размер файла: {result['file_size_bytes']:,} байт")
            print(f"   Время до ошибки: "
                  f"{result['processing_time_seconds']:.2f} сек")
            print(f"   Начало: {result['start_time']}")
            print(f"   Ошибка: {result.get('error', 'Unknown error')}")

            steps = result.get('steps_completed', [])
            if steps:
                print(f"   Выполнено шагов до ошибки ({len(steps)}): "
                      f"{', '.join(steps)}")

        print("\n" + "=" * 80)

    if deleted_files:
        print("\n" + "=" * 80)
        print(f"ФАЙЛЫ УДАЛЕНЫ ({len(deleted_files)})")
        print("=" * 80)
        for idx, result in enumerate(deleted_files, 1):
            print(f"\n{idx}. Файл: {result['file_name']}")
            print(f"   Бакет: {result['bucket']}")
            print(f"   Статус файла: {result['file_status']}")
            print(f"   Размер файла: {result['file_size_bytes']:,} байт")
            print(f"   Время обработки: "
                  f"{result['processing_time_seconds']:.2f} сек")
            print(f"   Начало: {result['start_time']}")
            print(f"   Конец: {result['end_time']}")
            print(
                "   Выполненные шаги ("
                f"{len(result.get('steps_completed', []))}"
                "): "
                f"{', '.join(result.get('steps_completed', []))}"
            )
            print(f"   Ошибка: {result.get('error', 'Unknown error')}")
            print(
                "   Выполненные шаги ("
                f"{len(result.get('steps_completed', []))}"
                "): "
                f"{', '.join(result.get('steps_completed', []))}"
            )

        print("\n" + "=" * 80)

    return {
        'total': len(results),
        'successful': len(successful),
        'failed': len(failed),
        'total_rows_loaded': total_rows,
        'total_processing_time_seconds': round(total_time, 2),
        'total_size_bytes': total_size,
    }
# =============================================================================
# DAG ОПРЕДЕЛЕНИЕ
# =============================================================================

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
    "forms_load_excel_to_starrocks",
    default_args=DEFAULT_ARGS,
    schedule=None,  # Manual trigger for testing
    catchup=False,
    tags=["forms", "starrocks", "excel", "form1", "form2"],
    max_active_runs=1,
    description="DAG для загрузки Financial Forms (Excel) в StarRocks",
) as dag:

    # Sensor for monitoring files in VK Cloud S3 (manual data folder)
    # With PostgreSQL logging enabled for forms_file_metadata tracking
    # Pattern supports any period descriptor (месяцев, квартал, etc.)
    vk_cloud_sensor = VKCloudS3Sensor(
        task_id="vk_cloud_s3_sensor",
        s3_buckets=[BUCKET_NAME],
        s3_prefix="manual-data/",
        s3_conn_id="rzdm_lake_s3",
        file_pattern=r'форма\s*[12].*\d{4}.*\.xls[xmb]?',
        postgres_conn_id="airflow_db",
        enable_logging=True,
        poke_interval=15,
        timeout=3600,
        mode="reschedule",
    )

    # Получение списка файлов для обработки
    get_changed_files_task = PythonOperator(
        task_id="get_changed_files",
        python_callable=get_changed_files,
    )

    get_missing_files_task = PythonOperator(
        task_id="get_missing_files",
        python_callable=get_missing_files,
    )

    # Параллельная обработка файлов (dynamic task mapping)
    process_changed_files = PythonOperator.partial(
        task_id="process_changed_file",
        python_callable=process_changed_file,
    ).expand(op_kwargs=get_changed_files_task.output.map(lambda f: {'file_data': f}))

    process_missing_files = PythonOperator.partial(
        task_id="process_missing_file",
        python_callable=process_missing_file,
    ).expand(op_kwargs=get_missing_files_task.output.map(lambda f: {'file_data': f}))

    # Подведение итогов (передаём список XComArg, без конкатенации операторов)
    summarize_task = PythonOperator(
        task_id="summarize_results",
        python_callable=summarize_results,
        op_kwargs={
            'changed_results': process_changed_files.output,
            'missing_results': process_missing_files.output,
        },
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Run dbt models after data load
    dbt_run_financial = BashOperator(
        task_id="dbt_run_financial",
        bash_command=f"""
        cd /opt/airflow/dbt/financial_dbt_project && \
        dbt run --profiles-dir . --target rzdm_rdv --vars '{{"bucket_name": "{BUCKET_NAME_CLEAN}"}}'
        """,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )



    # Определение зависимостей
    # Зависимости без вложенных списков
    vk_cloud_sensor >> get_changed_files_task
    vk_cloud_sensor >> get_missing_files_task
    get_changed_files_task >> process_changed_files
    get_missing_files_task >> process_missing_files
    [process_changed_files, process_missing_files] >> summarize_task >> dbt_run_financial

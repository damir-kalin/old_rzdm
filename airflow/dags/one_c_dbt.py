from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sdk import task_group
from airflow.models import Variable

from cosmos import (
    DbtTaskGroup,
    ProfileConfig,
    ProjectConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode

# Конфигурация проекта dbt
# Используем кеширование для более быстрого парсинга при импорте DAG
project_config = ProjectConfig(
    dbt_project_path="/opt/airflow/dbt/1c_data_bus_dbt_project",
    models_relative_path="models",
)


def get_profile_config(target_name: str):
    """Создает ProfileConfig для указанного target"""
    return ProfileConfig(
        profile_name="one_c_data_bus_dbt_project",
        target_name=target_name,
        profiles_yml_filepath="/opt/airflow/dbt/1c_data_bus_dbt_project/profiles.yml",
    )


# Конфигурация выполнения - LOCAL режим для выполнения внутри Airflow
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
)

# Конфигурация рендеринга - будет переопределяться для каждой группы


def get_render_config(select_path: str):
    """Создает RenderConfig с фильтрацией по пути.

    Args:
        select_path: Путь для фильтрации моделей
            (например, "path:models/unverified_rdv/hubs")

    Returns:
        RenderConfig с настроенной фильтрацией
    """
    return RenderConfig(
        emit_datasets=True,
        select=[select_path],  # select должен быть списком
        # Тесты включены, но настроены с severity: warn,
        # поэтому они не будут ломать пайплайн
    )


# Аргументы по умолчанию для DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="1c_data_bus_dbt",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["dbt", "cosmos", "data_pipeline"],
    description=(
        "DAG для пошаговой трансформации данных через dbt с использованием "
        "Cosmos. Использует DbtTaskGroup для автоматического управления "
        "зависимостями моделей."
    ),
    max_active_runs=1,
    # Ограничение параллельных задач для предотвращения перегрузки
    max_active_tasks=2,
) as dag:

    # Начальная задача
    start = EmptyOperator(
        task_id="start",
        doc_md="Начало выполнения DAG трансформации данных",
    )

    unverified_rdv_group = DbtTaskGroup(
        group_id="unverified_rdv_group",
        project_config=project_config,
        profile_config=get_profile_config("unverified_rdv"),
        execution_config=execution_config,
        render_config=get_render_config("path:models/unverified_rdv/hubs"),
        operator_args={
            "select": "unverified_rdv.hubs+",
            "full_refresh": False,
            "exclude": "test_name:*relationships*",
        },
        dag=dag,
    )

    mart_dim_group = DbtTaskGroup(
        group_id="mart_dim_group",
        project_config=project_config,
        profile_config=get_profile_config("unverified_mart"),
        execution_config=execution_config,
        render_config=get_render_config("path:models/unverified_mart/dims"),
        operator_args={
            "select": "path:models/unverified_mart/dims+",
            "full_refresh": False,
        },
        dag=dag,
    )


    # Конечная задача
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Завершение выполнения DAG трансформации данных",
    )


    start >> unverified_rdv_group >> mart_dim_group >> end

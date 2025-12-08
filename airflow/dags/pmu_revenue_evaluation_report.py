from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from starrocks_operators.starrocks_to_postgres_operator import StarRocksToPostgresOperator

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}



with DAG(
    "pmu_revenue_evaluation_report",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=["dbt"],
    max_active_runs=1,
    description="DAG запуска дбт моделей, mart и report",
) as dag:
    
    start = BashOperator(task_id='start',
                                   bash_command = """cd / && ls opt/airflow/dbt/main_dbt_project/"""
                                   )                                 
    dbt_run_mart = BashOperator(task_id='dbt_run_mart',
                                   bash_command = """cd / && cd /opt/airflow/dbt/main_dbt_project/models && 
                                   dbt run --profiles-dir /opt/airflow/dbt/main_dbt_project --select rzdm_mart+ --target rzdm_mart"""
                                   ) 
                                
    dbt_run_report = BashOperator(task_id='dbt_run_report',
                                       bash_command = """cd / && cd /opt/airflow/dbt/main_dbt_project/models && 
                                   dbt run --profiles-dir /opt/airflow/dbt/main_dbt_project --select rzdm_report+ --target rzdm_report"""
                                   )                                   

    # Зависимости
    start >> dbt_run_mart >> dbt_run_report 
    

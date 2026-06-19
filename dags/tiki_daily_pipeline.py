from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"


default_args = {
    "owner": "tiki-lakehouse",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="tiki_daily_pipeline",
    description="Crawl Tiki product data daily and rebuild dbt lakehouse marts.",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 6, 5, tz="Asia/Bangkok"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["tiki", "lakehouse", "daily"],
) as dag:
    crawl_tiki_products = BashOperator(
        task_id="crawl_tiki_products",
        bash_command=f"set -e; cd {PROJECT_DIR}; python crawler/fetch_tiki_8322.py",
        execution_timeout=timedelta(hours=3),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"set -e; cd {PROJECT_DIR}/dbt; dbt run --target trino",
        execution_timeout=timedelta(hours=1),
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"set -e; cd {PROJECT_DIR}/dbt; dbt test --target trino",
        execution_timeout=timedelta(minutes=30),
    )

    crawl_tiki_products >> dbt_run_marts >> dbt_test_marts

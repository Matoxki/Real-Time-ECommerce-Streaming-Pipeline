from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


DAG_ID = "ecommerce_dbt_transformation"
DBT_PROJECT_DIR = "/opt/airflow/dbt_ecommerce"
DBT_VENV_DIR = "/opt/airflow/dbt_ecommerce_venv"


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description=(
        "Runs and tests the Snowflake dbt models for the "
        "e-commerce streaming pipeline."
    ),
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 7, 28, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "streaming", "dbt", "snowflake"],
) as dag:

    run_dbt_transformations = BashOperator(
        task_id="run_dbt_models",
        execution_timeout=timedelta(minutes=20),
        bash_command=f"""
            set -euo pipefail

            VENV_DIR="{DBT_VENV_DIR}"
            PROJECT_DIR="{DBT_PROJECT_DIR}"

            if [ ! -x "$VENV_DIR/bin/dbt" ]; then
                python3 -m venv "$VENV_DIR"
                "$VENV_DIR/bin/pip" install --no-cache-dir --upgrade pip
                "$VENV_DIR/bin/pip" install --no-cache-dir dbt-snowflake
            fi

            cd "$PROJECT_DIR"

            "$VENV_DIR/bin/dbt" run --profiles-dir . --target dev
            "$VENV_DIR/bin/dbt" test --profiles-dir . --target dev
        """,
    )
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Defining default settings for our Airflow pipeline.
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Creating the DAG (Directed Acyclic Graph) for the streaming pipeline
with DAG(
    'ecommerce_dbt_transformation',
    default_args=default_args,
    description='Triggers dbt models hourly to process streaming e-commerce data',
    schedule_interval='@hourly',  # Runs once an hour
    start_date=datetime(2026, 7, 28),
    catchup=False,
    tags=['ecommerce', 'modern_data_stack', 'dbt', 'snowflake'],
) as dag:

    # Task: Create venv, install Snowflake adapter, and run dbt
    run_dbt_transformations = BashOperator(
        task_id='run_dbt_models',
        bash_command=(
            # 1. Create a dedicated virtual environment for this specific project
            'python3 -m venv /opt/airflow/dbt_ecommerce_venv && '
            
            # 2. Activate the environment
            'source /opt/airflow/dbt_ecommerce_venv/bin/activate && '
            
            # 3. Install the Snowflake adapter
            'pip install --no-cache-dir dbt-snowflake && '
            
            # 4. Navigate to your mapped dbt project folder
            'cd /opt/airflow/dbt_ecommerce && '
            
            # 5. Execute dbt commands using the local profiles.yml
            'dbt deps --profiles-dir . && '
            'dbt run --profiles-dir . && '
            'dbt test --profiles-dir .'
        )
    )

    # In this pipeline, Snowpipe handles the ingestion, so dbt is our only Airflow task.
    run_dbt_transformations
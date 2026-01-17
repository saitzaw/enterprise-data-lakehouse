"""
Airflow DAG for executing dbt transformations to ClickHouse gold layer.

This DAG:
1. Runs dbt models targeting ClickHouse (fact + dimension tables)
2. Executes dbt tests for data quality validation
3. Generates dbt documentation

Schedule: Daily at 02:00 UTC (after silver layer is ready)
Dependencies: Silver zone must be populated with Delta tables
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Default DAG arguments
default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['data-team@company.com'],
    'execution_timeout': timedelta(hours=2),
}

# DAG definition
dag = DAG(
    dag_id='clickhouse_gold_transformation',
    default_args=default_args,
    description='dbt transformations for ClickHouse gold layer analytics',
    schedule_interval='0 2 * * *',  # Daily at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gold', 'clickhouse', 'dbt', 'analytics'],
    max_active_runs=1,  # Prevent concurrent runs
)

with dag:
    # Task group for dbt operations
    with TaskGroup('dbt_operations', tooltip='dbt transformations and tests') as dbt_ops:
        
        # Run dbt models for gold layer
        dbt_run = BashOperator(
            task_id='dbt_run_gold_models',
            bash_command="""
                cd /opt/airflow/sap_dbt && \
                dbt run --select tag:gold --target clickhouse \
                    --vars '{
                        "execution_date": "{{ execution_date }}",
                        "execution_id": "{{ run_id }}"
                    }' && \
                echo "dbt run completed successfully"
            """,
            env={
                'DBT_PROFILES_DIR': '/opt/airflow/sap_dbt/profiles',
                'DBT_LOG_PATH': '/opt/airflow/logs/dbt',
                'PYTHONPATH': '/opt/spark/python:/opt/airflow',
            },
            append_env=True,
        )
        
        # Run dbt tests for quality validation
        dbt_test = BashOperator(
            task_id='dbt_test_gold_models',
            bash_command="""
                cd /opt/airflow/sap_dbt && \
                dbt test --select tag:gold --target clickhouse && \
                echo "All dbt tests passed"
            """,
            env={
                'DBT_PROFILES_DIR': '/opt/airflow/sap_dbt/profiles',
                'DBT_LOG_PATH': '/opt/airflow/logs/dbt',
            },
            append_env=True,
            trigger_rule='all_done',  # Run even if dbt_run fails
        )
        
        # Generate dbt documentation
        dbt_docs = BashOperator(
            task_id='dbt_generate_docs',
            bash_command="""
                cd /opt/airflow/sap_dbt && \
                dbt docs generate --target clickhouse && \
                echo "dbt docs generated at /opt/airflow/sap_dbt/target/index.html"
            """,
            env={
                'DBT_PROFILES_DIR': '/opt/airflow/sap_dbt/profiles',
            },
            append_env=True,
        )
        
        # Task dependencies within task group
        dbt_run >> dbt_test >> dbt_docs

    # Verification task - query ClickHouse to confirm table population
    with TaskGroup('validation', tooltip='Post-transformation validation') as validation:
        
        validate_fact_table = BashOperator(
            task_id='validate_fact_sales_orders',
            bash_command="""
                docker exec clickhouse clickhouse-client \
                    -u default \
                    -p clickhousepass \
                    --query "SELECT COUNT(*) as row_count FROM fact_sales_orders_ch" \
                    --format TabSeparated
            """,
        )
        
        validate_customer_dim = BashOperator(
            task_id='validate_dim_customers',
            bash_command="""
                docker exec clickhouse clickhouse-client \
                    -u default \
                    -p clickhousepass \
                    --query "SELECT COUNT(*) as row_count FROM dim_customers_ch" \
                    --format TabSeparated
            """,
        )
        
        validate_date_dim = BashOperator(
            task_id='validate_dim_date',
            bash_command="""
                docker exec clickhouse clickhouse-client \
                    -u default \
                    -p clickhousepass \
                    --query "SELECT COUNT(*) as row_count FROM dim_date_ch" \
                    --format TabSeparated
            """,
        )

    # Success notification
    success_notification = BashOperator(
        task_id='send_success_alert',
        bash_command='echo "ClickHouse gold layer transformation completed successfully at {{ execution_date }}"',
        trigger_rule='all_success',
    )

    # Failure notification
    failure_notification = BashOperator(
        task_id='send_failure_alert',
        bash_command="""
            echo "ClickHouse gold layer transformation FAILED at {{ execution_date }}"
            echo "Check logs: docker logs airflow-webserver"
        """,
        trigger_rule='one_failed',
    )

    # DAG execution flow
    dbt_ops >> validation >> [success_notification, failure_notification]

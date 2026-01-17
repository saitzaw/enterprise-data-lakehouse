# kafka_cdc_ingestion.py : version 1.00 Date: 2025-01-17
#######################################################################
# Kafka CDC to Delta Lake Ingestion using BashOperator
#######################################################################
#    Scheduler process for consuming Kafka CDC topics and writing
#    to Bronze zone in Delta format.
#    version: 1.00 Author: Sai Thiha Zaw Date: 2025-01-17 Create
#######################################################################
#   Kafka Topics:
# 1. sap_customers - Customer master data changes (CDC from PostgreSQL)
# 2. sap_sales - Sales order changes (CDC)
# 3. sap_cdc - Generic CDC topic for all tables
#######################################################################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="kafka_cdc_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Triggered manually or by external event
    catchup=False,
    default_args=default_args,
    tags=["kafka", "cdc", "bronze", "streaming"]
) as dag:

    # Customer data CDC streaming
    customers_cdc_task = BashOperator(
        task_id="customers_cdc_to_bronze",
        bash_command='''
            bash -lc "
            spark-submit \\
                --master spark://spark-master:7077 \\
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \\
                --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
                --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
                /opt/spark/jobs/pyspark/src/kafka_consumer.py \\
                --topic sap_customers \\
                --mode streaming \\
                --bootstrap-servers kafka:29092 \\
                --delta-path /opt/spark/data/bronze/kafka_customers
            "
        ''',
    )

    # Sales data CDC streaming
    sales_cdc_task = BashOperator(
        task_id="sales_cdc_to_bronze",
        bash_command='''
            bash -lc "
            spark-submit \\
                --master spark://spark-master:7077 \\
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \\
                --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
                --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
                /opt/spark/jobs/pyspark/src/kafka_consumer.py \\
                --topic sap_sales \\
                --mode streaming \\
                --bootstrap-servers kafka:29092 \\
                --delta-path /opt/spark/data/bronze/kafka_sales
            "
        ''',
    )

    # Generic CDC topic for all other tables
    generic_cdc_task = BashOperator(
        task_id="generic_cdc_to_bronze",
        bash_command='''
            bash -lc "
            spark-submit \\
                --master spark://spark-master:7077 \\
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \\
                --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \\
                --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \\
                /opt/spark/jobs/pyspark/src/kafka_consumer.py \\
                --topic sap_cdc \\
                --mode batch \\
                --bootstrap-servers kafka:29092 \\
                --delta-path /opt/spark/data/bronze/kafka_cdc
            "
        ''',
    )

    # Task dependencies - can run in parallel
    [customers_cdc_task, sales_cdc_task, generic_cdc_task]

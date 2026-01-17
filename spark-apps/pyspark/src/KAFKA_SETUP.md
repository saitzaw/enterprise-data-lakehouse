# Kafka Producer/Consumer Setup Guide

## Overview

This guide demonstrates how to use Kafka as a real-time data source for the data lakehouse. The setup includes:

- **Kafka Producer** (`kafka_producer.py`) - Sends data to Kafka topics
- **Kafka Consumer** (`kafka_consumer.py`) - Reads from Kafka and writes to Delta Lake
- **Airflow DAG** (`kafka_cdc_ingestion.py`) - Orchestrates streaming pipelines

## Architecture

```
PostgreSQL/SAP → Debezium CDC → Kafka → Consumer → Delta Lake (Bronze)
                                       ↓
                                  Batch/Streaming
```

## Prerequisites

Ensure Kafka infrastructure is running:

```bash
make dev-streaming-up    # Kafka + Debezium + Spark + PostgreSQL
# or
make dev-up-all-services  # All services except Elasticsearch
```

Verify services:
```bash
curl http://localhost:9021              # Kafka Control Center
curl http://localhost:8083/connector-plugins  # Debezium
```

## Setting Up Debezium CDC

### 1. Enable PostgreSQL WAL and Logical Decoding

```bash
make dev-postgres-shell

# Inside postgres container:
psql -U sparkuser -d sparkdb

-- Check WAL settings
SHOW wal_level;  -- Should be 'logical' or higher
SHOW max_wal_senders;
SHOW max_replication_slots;
```

### 2. Create Debezium Connector

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pg-sap-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "sparkuser",
      "database.password": "s3cureP@ssw0rd",
      "database.dbname": "sparkdb",
      "database.server.name": "sap_server",
      "schema.include.list": "public",
      "table.include.list": "public.vbak,public.vbap,public.vbep,public.kna1",
      "plugin.name": "pgoutput",
      "publication.name": "debezium_pub",
      "slot.name": "debezium_slot",
      "slot.drop.on.stop": false,
      "publication.autocreate.mode": "all_tables",
      "decimal.handling.mode": "string"
    }
  }'
```

### 3. Verify Connector Status

```bash
# List all connectors
curl http://localhost:8083/connectors

# Check specific connector status
curl http://localhost:8083/connectors/pg-sap-connector/status

# View connector logs
curl http://localhost:8083/connectors/pg-sap-connector/tasks
```

## Producer Examples

### 1. Test Producer with Sample Data

```bash
# Start Spark environment
make dev-spark-shell

# Submit producer job with sample data
spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/pyspark/src/kafka_producer.py \
  --topic sap_sales \
  --sample \
  --sample-count 10
```

### 2. Verify Data in Kafka (Using Kafka Control Center)

```bash
# Access Kafka Control Center
http://localhost:9021

# Navigate to: Topics → sap_sales → Messages
# Should see 10 sample order messages
```

### 3. Manual Topic Creation

```bash
# Inside Kafka container
docker exec -it kafka \
  kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic sap_customers \
    --partitions 3 \
    --replication-factor 1 \
    --config retention.ms=86400000
```

## Consumer Examples

### 1. Batch Processing Mode

```bash
# Process Kafka messages in 60-second micro-batches
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /opt/spark/jobs/pyspark/src/kafka_consumer.py \
  --topic sap_sales \
  --mode batch \
  --delta-path /opt/spark/data/bronze/kafka_sales
```

### 2. Continuous Streaming Mode

```bash
# Process Kafka messages in real-time (continuous)
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /opt/spark/jobs/pyspark/src/kafka_consumer.py \
  --topic sap_customers \
  --mode streaming \
  --delta-path /opt/spark/data/bronze/kafka_customers
```

### 3. Using Jupyter Notebook

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "sap_sales") \
    .option("startingOffsets", "latest") \
    .load()

# View schema
df.printSchema()

# Display sample data
df.select("value").writeStream \
    .format("console") \
    .start() \
    .awaitTermination(timeout=10000)
```

## Airflow Orchestration

### 1. Trigger Kafka Ingestion DAG

```bash
# Access Airflow UI
http://localhost:8088

# Navigate to: DAGs → kafka_cdc_ingestion
# Click: Trigger DAG

# Or trigger via CLI
make dev-airflow-shell

# Inside Airflow container:
airflow dags trigger kafka_cdc_ingestion

# Monitor execution
airflow dags list-runs --dag-id kafka_cdc_ingestion
```

### 2. Monitor Streaming Queries

```bash
# Inside Spark shell
make dev-spark-shell

# List active queries
spark.streams.active

# Get specific query metrics
query = spark.streams.active[0]
print(query.status)
print(query.processedRowsPerSecond)
print(query.isActive)
```

## Monitoring & Debugging

### 1. Check Kafka Topic Lag

```bash
# Inside Kafka container
docker exec -it kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group sap-consumer-group \
    --describe
```

### 2. View Kafka Messages (CLI)

```bash
docker exec -it kafka \
  kafka-console-consumer --bootstrap-server localhost:9092 \
    --topic sap_sales \
    --from-beginning \
    --max-messages 10
```

### 3. Check Delta Table Contents

```bash
# In Jupyter or Spark shell
spark.read.format("delta") \
    .load("/opt/spark/data/bronze/kafka_sales") \
    .show()

# Check schema
spark.read.format("delta") \
    .load("/opt/spark/data/bronze/kafka_sales") \
    .printSchema()

# Count records
spark.read.format("delta") \
    .load("/opt/spark/data/bronze/kafka_sales") \
    .count()
```

### 4. View Consumer Logs

```bash
# Spark driver logs
make dev-spark-logs

# Airflow task logs
make dev-airflow-logs

# Check checkpoint progress
ls -la /opt/spark/checkpoints/sap_sales/
```

## Common Issues

### Issue: "Task timed out"

**Solution**: Streaming tasks run indefinitely. Use batch mode for Airflow DAGs:
```python
--mode batch  # Instead of streaming
```

### Issue: "Kafka broker not reachable"

**Solution**: Verify Kafka is running and Docker network:
```bash
docker inspect kafka --format '{{json .NetworkSettings.Networks}}'
docker exec spark-master nc -zv kafka 29092
```

### Issue: "Delta table not found"

**Solution**: Ensure path exists and Hive table is registered:
```bash
spark-sql
> CREATE TABLE IF NOT EXISTS bronze.kafka_sales \
  USING DELTA LOCATION '/opt/spark/data/bronze/kafka_sales'
> SELECT * FROM bronze.kafka_sales LIMIT 5;
```

### Issue: "Checkpoint directory permission denied"

**Solution**: Fix directory permissions:
```bash
sudo chown -R 1000:1000 /opt/spark/checkpoints/
sudo chmod -R 755 /opt/spark/checkpoints/
```

## Best Practices

1. **Use Batch Mode for Airflow**: Streaming queries don't work well in DAG orchestration. Use batch mode with scheduled intervals instead.

2. **Set Key for Partitioning**: Include message keys in producer for better Kafka partitioning:
   ```python
   send_message(producer, topic, data, key=customer_id)
   ```

3. **Monitor Lag**: Check consumer lag regularly to ensure timely processing:
   ```bash
   kafka-consumer-groups --describe --group <group-id>
   ```

4. **Schema Evolution**: Use Delta Lake's schema evolution for handling new columns:
   ```python
   .option("mergeSchema", "true")
   ```

5. **Exactly-Once Semantics**: Delta Lake + Spark Structured Streaming provides exactly-once guarantee with checkpointing.

## Next Steps

- Implement Silver layer transformation on Kafka-ingested data
- Add schema registry integration for Avro/Protobuf support
- Set up alerts for consumer lag or streaming failures
- Integrate with OpenLineage for data lineage tracking

#!/usr/bin/env python3
"""
kafka_consumer.py - Version 1.00 Date: 2025-01-17

Kafka Consumer for streaming SAP CDC data to Delta Lake Bronze zone.
Consumes messages from Kafka topics and writes to Delta tables via Spark.

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
        kafka_consumer.py --topic sap_cdc --mode batch
    
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
        kafka_consumer.py --topic sap_sales --mode streaming

Requirements:
    - Spark 3.5.5 with Kafka support
    - Delta Lake
    - python-dotenv
"""

import os
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, schema_of_json, current_timestamp, 
    window, count, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, MapType
)


# =========================
# --- Logging Setup ---
# =========================
def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure logging for Kafka consumer."""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


logger = setup_logging()


# =========================
# --- Config Loading ---
# =========================
def load_config(env_path: Path | None = None) -> Dict[str, str]:
    """Load configuration from environment variables."""
    if env_path:
        load_dotenv(env_path)
    else:
        default_env = Path(__file__).resolve().parent.parent / ".env"
        load_dotenv(default_env)
    
    config = {
        "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "sap_cdc"),
        "SPARK_MASTER": os.getenv("SPARK_MASTER", "spark://spark-master:7077"),
        "DELTA_BRONZE_PATH": os.getenv("DELTA_BRONZE_PATH", "/opt/spark/data/bronze"),
        "CHECKPOINT_PATH": os.getenv("CHECKPOINT_PATH", "/opt/spark/checkpoints"),
    }
    
    logger.info("Config loaded successfully")
    return config


# =========================
# --- Spark Session Setup ---
# =========================
def build_spark(cfg: Dict) -> SparkSession:
    """Create Spark session with Kafka support."""
    try:
        spark = SparkSession.builder \
            .appName("KafkaConsumer") \
            .master(cfg["SPARK_MASTER"]) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark
        
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise


# =========================
# --- Kafka Source ---
# =========================
def create_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    starting_offsets: str = "latest"
) -> DataFrame:
    """
    Create Kafka source for streaming.
    
    Args:
        spark: SparkSession
        bootstrap_servers: Kafka broker addresses (comma-separated)
        topic: Topic to consume
        starting_offsets: "latest", "earliest", or JSON with offsets
    
    Returns:
        DataFrame with Kafka messages
    """
    try:
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", starting_offsets) \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Cast key and value to string
        df = df.select(
            col("key").cast(StringType()).alias("kafka_key"),
            col("value").cast(StringType()).alias("kafka_value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp")
        )
        
        logger.info(f"Kafka stream created for topic: {topic}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to create Kafka stream: {e}")
        raise


# =========================
# --- Schema Inference ---
# =========================
def get_kafka_message_schema() -> StructType:
    """Define schema for Kafka messages from producer."""
    return StructType([
        StructField("timestamp", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField("data", StructType([
            StructField("operation", StringType(), True),
            StructField("table", StringType(), True),
            StructField("before", MapType(StringType(), StringType()), True),
            StructField("after", MapType(StringType(), StringType()), True),
            StructField("timestamp_ms", IntegerType(), True),
        ]), True)
    ])


# =========================
# --- Message Parsing ---
# =========================
def parse_kafka_messages(df: DataFrame) -> DataFrame:
    """Parse Kafka JSON values into structured data."""
    try:
        schema = get_kafka_message_schema()
        
        parsed_df = df.select(
            col("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("kafka_value"), schema).alias("message")
        ) \
        .select(
            col("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            col("message.timestamp").alias("message_timestamp"),
            col("message.source_system"),
            col("message.data.operation").alias("operation"),
            col("message.data.table").alias("source_table"),
            col("message.data.before").alias("before_state"),
            col("message.data.after").alias("after_state"),
            col("message.data.timestamp_ms").alias("change_timestamp_ms")
        )
        
        logger.info("Messages parsed successfully")
        return parsed_df
        
    except Exception as e:
        logger.error(f"Failed to parse messages: {e}")
        raise


# =========================
# --- Data Transformation ---
# =========================
def enrich_messages(df: DataFrame) -> DataFrame:
    """Add processing metadata to messages."""
    return df.select(
        col("kafka_key"),
        col("source_table"),
        col("operation"),
        col("before_state"),
        col("after_state"),
        col("kafka_timestamp"),
        current_timestamp().alias("processed_at"),
        col("partition"),
        col("offset")
    )


# =========================
# --- Delta Write ---
# =========================
def write_delta_batch(
    df: DataFrame,
    delta_path: str,
    table_name: str,
    mode: str = "append",
    partition_cols: Optional[list] = None
) -> None:
    """
    Write DataFrame to Delta table in batch mode.
    
    Args:
        df: DataFrame to write
        delta_path: Path to Delta table
        table_name: Name for Hive table registration
        mode: "append", "overwrite", "ignore"
        partition_cols: Columns to partition by
    """
    try:
        write_config = df.write \
            .format("delta") \
            .mode(mode)
        
        if partition_cols:
            write_config = write_config.partitionBy(*partition_cols)
        
        write_config.save(delta_path)
        
        # Register in Hive metastore
        spark = df.sparkSession
        spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS bronze.{table_name} "
            f"USING DELTA LOCATION '{delta_path}'"
        )
        
        logger.info(f"Data written to Delta: {delta_path}")
        
    except Exception as e:
        logger.error(f"Failed to write Delta data: {e}")
        raise


# =========================
# --- Streaming Write ---
# =========================
def write_delta_streaming(
    df: DataFrame,
    delta_path: str,
    checkpoint_path: str,
    table_name: str,
    mode: str = "append"
) -> None:
    """
    Write DataFrame to Delta in streaming mode.
    
    Args:
        df: Streaming DataFrame
        delta_path: Target Delta table path
        checkpoint_path: Checkpoint location for fault tolerance
        table_name: Name for Hive table
        mode: "append" (default) or "complete"
    """
    try:
        query = df.writeStream \
            .format("delta") \
            .mode(mode) \
            .option("checkpointLocation", checkpoint_path) \
            .start(delta_path)
        
        logger.info(f"Streaming write started: {delta_path}")
        logger.info(f"Checkpoint: {checkpoint_path}")
        
        return query
        
    except Exception as e:
        logger.error(f"Failed to start streaming write: {e}")
        raise


# =========================
# --- Batch Processing ---
# =========================
def process_batch(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    delta_path: str,
    batch_duration: int = 60
) -> None:
    """
    Process Kafka messages in batch mode.
    
    Args:
        spark: SparkSession
        bootstrap_servers: Kafka brokers
        topic: Kafka topic
        delta_path: Delta output path
        batch_duration: Batch window in seconds
    """
    try:
        logger.info(f"Starting batch processing for {topic}")
        
        # Create stream and parse
        kafka_df = create_kafka_stream(spark, bootstrap_servers, topic, "latest")
        messages_df = parse_kafka_messages(kafka_df)
        enriched_df = enrich_messages(messages_df)
        
        # Batch write with 1-minute micro-batches
        query = enriched_df.writeStream \
            .format("delta") \
            .mode("append") \
            .option("checkpointLocation", f"{delta_path}/_checkpoint") \
            .option("mergeSchema", "true") \
            .trigger(processingTime=f"{batch_duration} seconds") \
            .start(delta_path)
        
        logger.info(f"Batch processing query started for {topic}")
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Batch processing interrupted by user")
        if 'query' in locals():
            query.stop()
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        raise


# =========================
# --- Streaming Processing ---
# =========================
def process_streaming(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    delta_path: str,
    checkpoint_path: str
) -> None:
    """
    Process Kafka messages in continuous streaming mode.
    
    Args:
        spark: SparkSession
        bootstrap_servers: Kafka brokers
        topic: Kafka topic
        delta_path: Delta output path
        checkpoint_path: Checkpoint location
    """
    try:
        logger.info(f"Starting streaming processing for {topic}")
        
        # Create stream and parse
        kafka_df = create_kafka_stream(spark, bootstrap_servers, topic, "earliest")
        messages_df = parse_kafka_messages(kafka_df)
        enriched_df = enrich_messages(messages_df)
        
        # Streaming write
        query = write_delta_streaming(
            enriched_df,
            delta_path,
            checkpoint_path,
            f"kafka_{topic}",
            mode="append"
        )
        
        logger.info(f"Streaming query ID: {query.id}")
        logger.info(f"Streaming query status: {query.status}")
        
        # Wait for termination
        query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Streaming processing interrupted by user")
        if 'query' in locals():
            query.stop()
    except Exception as e:
        logger.error(f"Streaming processing failed: {e}")
        raise


# =========================
# --- Main Entry Point ---
# =========================
def main():
    """Main consumer entrypoint."""
    parser = argparse.ArgumentParser(
        description="Kafka Consumer for SAP CDC to Delta Lake"
    )
    parser.add_argument(
        "--topic",
        default="sap_cdc",
        help="Kafka topic to consume"
    )
    parser.add_argument(
        "--mode",
        choices=["batch", "streaming"],
        default="batch",
        help="Processing mode"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka:29092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--delta-path",
        default="/opt/spark/data/bronze/kafka_cdc",
        help="Delta table output path"
    )
    
    args = parser.parse_args()
    
    try:
        # Load config
        config = load_config()
        bootstrap_servers = args.bootstrap_servers or config["KAFKA_BOOTSTRAP_SERVERS"]
        topic = args.topic or config["KAFKA_TOPIC"]
        delta_path = args.delta_path or f"{config['DELTA_BRONZE_PATH']}/{topic}"
        checkpoint_path = f"{config['CHECKPOINT_PATH']}/{topic}"
        
        # Create Spark session
        spark = build_spark(config)
        
        # Process based on mode
        if args.mode == "streaming":
            process_streaming(spark, bootstrap_servers, topic, delta_path, checkpoint_path)
        else:
            process_batch(spark, bootstrap_servers, topic, delta_path)
        
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
kafka_producer.py - Version 1.00 Date: 2025-01-17

Kafka Producer for streaming SAP data changes to Kafka topics.
Consumes Debezium CDC messages and forwards them to Kafka topics for real-time processing.

Usage:
    python kafka_producer.py --topic sap_customers --source postgresql
    python kafka_producer.py --topic sap_sales --source postgresql --batch-size 100

Requirements:
    pip install kafka-python python-dotenv
"""

import json
import os
import argparse
import logging
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError


# =========================
# --- Logging Setup ---
# =========================
def setup_logging(log_level: str = "INFO") -> logging.Logger:
    """Configure logging for Kafka producer."""
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
        "SOURCE_SYSTEM": os.getenv("SOURCE_SYSTEM", "SAP"),
        "ENVIRONMENT": os.getenv("ENVIRONMENT", "dev"),
    }
    
    # Validate required configs
    if not config["KAFKA_BOOTSTRAP_SERVERS"]:
        raise ValueError("KAFKA_BOOTSTRAP_SERVERS not set in environment")
    
    logger.info(f"Config loaded: Kafka={config['KAFKA_BOOTSTRAP_SERVERS']}")
    return config


# =========================
# --- Kafka Producer Setup ---
# =========================
def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Create Kafka producer with JSON serialization."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,
            max_in_flight_requests_per_connection=1,  # Ensure ordering
        )
        logger.info(f"Kafka producer created for {bootstrap_servers}")
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise


# =========================
# --- Message Production ---
# =========================
def send_message(
    producer: KafkaProducer,
    topic: str,
    data: Dict,
    key: Optional[str] = None
) -> bool:
    """
    Send a message to Kafka topic.
    
    Args:
        producer: KafkaProducer instance
        topic: Target Kafka topic
        data: Message payload (dict)
        key: Optional message key for partitioning
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Add metadata
        message = {
            "timestamp": datetime.utcnow().isoformat(),
            "source_system": "SAP",
            "data": data
        }
        
        future = producer.send(topic, value=message, key=key)
        record_metadata = future.get(timeout=10)
        
        logger.debug(
            f"Message sent to {record_metadata.topic} "
            f"[partition {record_metadata.partition}, offset {record_metadata.offset}]"
        )
        return True
        
    except KafkaError as e:
        logger.error(f"Kafka error while sending to {topic}: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error while sending to {topic}: {e}")
        return False


# =========================
# --- Batch Production ---
# =========================
def send_batch(
    producer: KafkaProducer,
    topic: str,
    messages: list,
    key_field: Optional[str] = None
) -> Dict[str, int]:
    """
    Send multiple messages to Kafka topic.
    
    Args:
        producer: KafkaProducer instance
        topic: Target Kafka topic
        messages: List of message dicts
        key_field: Optional field to use as Kafka message key
    
    Returns:
        Dict with success/failure counts
    """
    stats = {"sent": 0, "failed": 0}
    
    for msg in messages:
        key = str(msg.get(key_field)) if key_field and key_field in msg else None
        
        if send_message(producer, topic, msg, key):
            stats["sent"] += 1
        else:
            stats["failed"] += 1
    
    logger.info(f"Batch complete: {stats['sent']} sent, {stats['failed']} failed")
    return stats


# =========================
# --- CDC Message Handler ---
# =========================
def process_cdc_message(cdc_payload: Dict, topic_mapping: Dict[str, str]) -> Optional[Dict]:
    """
    Transform Debezium CDC message to standardized Kafka message.
    
    CDC payload format from Debezium:
    {
        "before": {...},  # Previous state
        "after": {...},   # Current state
        "source": {...},  # Source metadata (table, schema, etc)
        "op": "c|u|d",    # Operation: create, update, delete
        "ts_ms": 1234567890
    }
    """
    try:
        operation = cdc_payload.get("op")  # c=create, u=update, d=delete
        source_table = cdc_payload.get("source", {}).get("table")
        
        # Route to appropriate topic based on source table
        target_topic = topic_mapping.get(source_table, "sap_cdc_default")
        
        message = {
            "operation": operation,
            "table": source_table,
            "before": cdc_payload.get("before"),
            "after": cdc_payload.get("after"),
            "timestamp_ms": cdc_payload.get("ts_ms"),
            "debezium_source": cdc_payload.get("source"),
        }
        
        return {
            "topic": target_topic,
            "key": str(cdc_payload.get("after", {}).get("id")) if operation != "d" else None,
            "data": message
        }
    except Exception as e:
        logger.error(f"Failed to process CDC message: {e}")
        return None


# =========================
# --- Sample Data Production ---
# =========================
def produce_sample_sales_data(producer: KafkaProducer, topic: str, count: int = 5):
    """Produce sample SAP sales order data for testing."""
    sample_orders = [
        {
            "order_id": f"SO{i:06d}",
            "customer_id": f"CUST{i:04d}",
            "order_date": datetime.utcnow().isoformat(),
            "amount": 1000 * i,
            "currency": "USD",
            "status": "OPEN"
        }
        for i in range(1, count + 1)
    ]
    
    stats = send_batch(
        producer, 
        topic, 
        sample_orders,
        key_field="order_id"
    )
    logger.info(f"Sample data produced: {stats}")


# =========================
# --- Main Entry Point ---
# =========================
def main():
    """Main producer entrypoint."""
    parser = argparse.ArgumentParser(
        description="Kafka Producer for SAP CDC data"
    )
    parser.add_argument(
        "--topic",
        default="sap_cdc",
        help="Target Kafka topic"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="kafka:29092",
        help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Produce sample data for testing"
    )
    parser.add_argument(
        "--sample-count",
        type=int,
        default=5,
        help="Number of sample messages to produce"
    )
    
    args = parser.parse_args()
    
    try:
        # Load config
        config = load_config()
        bootstrap_servers = args.bootstrap_servers or config["KAFKA_BOOTSTRAP_SERVERS"]
        topic = args.topic or config["KAFKA_TOPIC"]
        
        # Create producer
        producer = create_producer(bootstrap_servers)
        
        if args.sample:
            logger.info(f"Producing {args.sample_count} sample messages to {topic}")
            produce_sample_sales_data(producer, topic, args.sample_count)
        else:
            logger.info(f"Producer ready. Listening for messages on {topic}")
            # In production, would connect to Debezium/CDC source here
        
        producer.flush()
        logger.info("Producer flushed successfully")
        
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        raise
    finally:
        if 'producer' in locals():
            producer.close()


if __name__ == "__main__":
    main()

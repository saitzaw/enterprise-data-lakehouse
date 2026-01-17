#!/bin/bash

# kafka_operations.sh - Kafka Management Utilities
# Quick commands for Kafka producer/consumer/topic management
# Usage: ./kafka_operations.sh <command> [args]

set -e

KAFKA_CONTAINER="kafka"
BOOTSTRAP_SERVERS="localhost:9092"
DOCKER_BOOTSTRAP="kafka:29092"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# =========================
# Helper Functions
# =========================
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# =========================
# Topic Management
# =========================
create_topic() {
    local topic=$1
    local partitions=${2:-3}
    local replication=${3:-1}
    
    if [ -z "$topic" ]; then
        print_error "Topic name required"
        return 1
    fi
    
    print_info "Creating topic: $topic (partitions=$partitions, replication=$replication)"
    docker exec -it "$KAFKA_CONTAINER" \
        kafka-topics --create \
            --bootstrap-server localhost:9092 \
            --topic "$topic" \
            --partitions "$partitions" \
            --replication-factor "$replication" \
            --if-not-exists
}

list_topics() {
    print_info "Listing Kafka topics..."
    docker exec "$KAFKA_CONTAINER" \
        kafka-topics --list \
            --bootstrap-server localhost:9092
}

describe_topic() {
    local topic=$1
    
    if [ -z "$topic" ]; then
        print_error "Topic name required"
        return 1
    fi
    
    print_info "Topic details: $topic"
    docker exec "$KAFKA_CONTAINER" \
        kafka-topics --describe \
            --bootstrap-server localhost:9092 \
            --topic "$topic"
}

delete_topic() {
    local topic=$1
    
    if [ -z "$topic" ]; then
        print_error "Topic name required"
        return 1
    fi
    
    print_warn "Deleting topic: $topic"
    docker exec -it "$KAFKA_CONTAINER" \
        kafka-topics --delete \
            --bootstrap-server localhost:9092 \
            --topic "$topic"
}

# =========================
# Producer Operations
# =========================
test_producer() {
    local topic=$1
    
    if [ -z "$topic" ]; then
        print_error "Topic name required"
        return 1
    fi
    
    print_info "Starting producer for topic: $topic (type messages, Ctrl+D to exit)"
    docker exec -it "$KAFKA_CONTAINER" \
        kafka-console-producer \
            --broker-list localhost:9092 \
            --topic "$topic"
}

# =========================
# Consumer Operations
# =========================
test_consumer() {
    local topic=$1
    local max_messages=${2:-10}
    
    if [ -z "$topic" ]; then
        print_error "Topic name required"
        return 1
    fi
    
    print_info "Consuming $max_messages messages from topic: $topic"
    docker exec "$KAFKA_CONTAINER" \
        kafka-console-consumer \
            --bootstrap-server localhost:9092 \
            --topic "$topic" \
            --from-beginning \
            --max-messages "$max_messages"
}

tail_topic() {
    local topic=$1
    
    if [ -z "$topic" ]; then
        print_error "Topic name required"
        return 1
    fi
    
    print_info "Tailing topic: $topic (latest messages)"
    docker exec -it "$KAFKA_CONTAINER" \
        kafka-console-consumer \
            --bootstrap-server localhost:9092 \
            --topic "$topic"
}

# =========================
# Consumer Group Management
# =========================
list_consumer_groups() {
    print_info "Listing consumer groups..."
    docker exec "$KAFKA_CONTAINER" \
        kafka-consumer-groups \
            --bootstrap-server localhost:9092 \
            --list
}

describe_consumer_group() {
    local group=$1
    
    if [ -z "$group" ]; then
        print_error "Consumer group name required"
        return 1
    fi
    
    print_info "Consumer group details: $group"
    docker exec "$KAFKA_CONTAINER" \
        kafka-consumer-groups \
            --bootstrap-server localhost:9092 \
            --group "$group" \
            --describe
}

reset_consumer_group() {
    local group=$1
    local topic=$2
    
    if [ -z "$group" ] || [ -z "$topic" ]; then
        print_error "Consumer group and topic required"
        return 1
    fi
    
    print_warn "Resetting consumer group: $group for topic: $topic"
    docker exec "$KAFKA_CONTAINER" \
        kafka-consumer-groups \
            --bootstrap-server localhost:9092 \
            --group "$group" \
            --topic "$topic" \
            --reset-offsets \
            --to-earliest \
            --execute
}

# =========================
# Debezium Operations
# =========================
list_debezium_connectors() {
    print_info "Listing Debezium connectors..."
    curl -s http://localhost:8083/connectors | jq '.'
}

describe_debezium_connector() {
    local connector=$1
    
    if [ -z "$connector" ]; then
        print_error "Connector name required"
        return 1
    fi
    
    print_info "Debezium connector details: $connector"
    curl -s http://localhost:8083/connectors/"$connector" | jq '.'
}

check_debezium_status() {
    local connector=$1
    
    if [ -z "$connector" ]; then
        print_error "Connector name required"
        return 1
    fi
    
    print_info "Debezium connector status: $connector"
    curl -s http://localhost:8083/connectors/"$connector"/status | jq '.'
}

delete_debezium_connector() {
    local connector=$1
    
    if [ -z "$connector" ]; then
        print_error "Connector name required"
        return 1
    fi
    
    print_warn "Deleting Debezium connector: $connector"
    curl -X DELETE http://localhost:8083/connectors/"$connector"
    print_info "Connector deleted"
}

# =========================
# Stats & Metrics
# =========================
show_broker_info() {
    print_info "Kafka broker information..."
    docker exec "$KAFKA_CONTAINER" \
        kafka-broker-api-versions --bootstrap-server localhost:9092
}

show_topic_stats() {
    local topic=$1
    
    if [ -z "$topic" ]; then
        print_error "Topic name required"
        return 1
    fi
    
    print_info "Topic statistics: $topic"
    docker exec "$KAFKA_CONTAINER" \
        kafka-log-dirs \
            --bootstrap-server localhost:9092 \
            --topic-list "$topic"
}

# =========================
# Spark Integration
# =========================
run_spark_producer() {
    print_info "Running Spark producer with sample data..."
    make dev-spark-shell || echo "make dev-spark-shell not available"
    
    spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/pyspark/src/kafka_producer.py \
        --topic sap_sales \
        --sample \
        --sample-count 10
}

run_spark_consumer() {
    local topic=${1:-sap_cdc}
    local mode=${2:-batch}
    
    print_info "Running Spark consumer: topic=$topic, mode=$mode"
    
    spark-submit \
        --master spark://spark-master:7077 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
        /opt/spark/jobs/pyspark/src/kafka_consumer.py \
        --topic "$topic" \
        --mode "$mode" \
        --bootstrap-servers "$DOCKER_BOOTSTRAP"
}

# =========================
# Help & Usage
# =========================
show_help() {
    cat << EOF
Kafka Management Utilities

USAGE:
    ./kafka_operations.sh <command> [args]

TOPIC COMMANDS:
    create_topic <name> [partitions] [replication]  Create a new topic
    list_topics                                     List all topics
    describe_topic <name>                           Show topic details
    delete_topic <name>                             Delete a topic

PRODUCER COMMANDS:
    test_producer <topic>                           Interactive producer
    run_spark_producer                              Send sample Spark data

CONSUMER COMMANDS:
    test_consumer <topic> [max_messages]            Read messages from topic
    tail_topic <topic>                              Tail topic (latest messages)
    run_spark_consumer [topic] [mode]               Start Spark consumer (batch|streaming)

CONSUMER GROUP COMMANDS:
    list_consumer_groups                            List all consumer groups
    describe_consumer_group <group>                 Show group details
    reset_consumer_group <group> <topic>            Reset consumer offsets

DEBEZIUM COMMANDS:
    list_debezium_connectors                        List all CDC connectors
    describe_debezium_connector <name>              Show connector config
    check_debezium_status <name>                    Check connector status
    delete_debezium_connector <name>                Delete a connector

STATS & INFO COMMANDS:
    show_broker_info                                Display broker API versions
    show_topic_stats <topic>                        Show topic storage stats

EXAMPLES:
    # Create topics for SAP data
    ./kafka_operations.sh create_topic sap_customers 3 1
    ./kafka_operations.sh create_topic sap_sales 3 1

    # List topics
    ./kafka_operations.sh list_topics

    # Test producer
    ./kafka_operations.sh test_producer sap_sales

    # Consume messages
    ./kafka_operations.sh test_consumer sap_sales 20

    # Check consumer lag
    ./kafka_operations.sh describe_consumer_group sap-consumer-group

    # Check Debezium status
    ./kafka_operations.sh check_debezium_status pg-sap-connector

    # Run Spark consumer
    ./kafka_operations.sh run_spark_consumer sap_sales batch
EOF
}

# =========================
# Main Entry Point
# =========================
main() {
    local command=$1
    
    case "$command" in
        # Topics
        create_topic)
            create_topic "$2" "$3" "$4"
            ;;
        list_topics)
            list_topics
            ;;
        describe_topic)
            describe_topic "$2"
            ;;
        delete_topic)
            delete_topic "$2"
            ;;
        
        # Producer
        test_producer)
            test_producer "$2"
            ;;
        run_spark_producer)
            run_spark_producer
            ;;
        
        # Consumer
        test_consumer)
            test_consumer "$2" "$3"
            ;;
        tail_topic)
            tail_topic "$2"
            ;;
        run_spark_consumer)
            run_spark_consumer "$2" "$3"
            ;;
        
        # Consumer Groups
        list_consumer_groups)
            list_consumer_groups
            ;;
        describe_consumer_group)
            describe_consumer_group "$2"
            ;;
        reset_consumer_group)
            reset_consumer_group "$2" "$3"
            ;;
        
        # Debezium
        list_debezium_connectors)
            list_debezium_connectors
            ;;
        describe_debezium_connector)
            describe_debezium_connector "$2"
            ;;
        check_debezium_status)
            check_debezium_status "$2"
            ;;
        delete_debezium_connector)
            delete_debezium_connector "$2"
            ;;
        
        # Stats
        show_broker_info)
            show_broker_info
            ;;
        show_topic_stats)
            show_topic_stats "$2"
            ;;
        
        # Help
        -h|--help|help)
            show_help
            ;;
        
        *)
            if [ -z "$command" ]; then
                show_help
            else
                print_error "Unknown command: $command"
                echo "Run './kafka_operations.sh help' for usage"
                exit 1
            fi
            ;;
    esac
}

main "$@"

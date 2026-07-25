#!/bin/bash

# ============================================================================
# Kafka Topic Initialization Script
# Creates all StreamMart topics with proper configuration
# ============================================================================

set -e

echo "Waiting for Kafka to be ready..."
sleep 10

KAFKA_BROKER="kafka:9092"

echo "Creating StreamMart Kafka topics..."

# Events topics - partitioned by session_id for sessionization
kafka-topics --create --if-not-exists \
  --bootstrap-server $KAFKA_BROKER \
  --topic events.pageview \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config cleanup.policy=delete

kafka-topics --create --if-not-exists \
  --bootstrap-server $KAFKA_BROKER \
  --topic events.product_click \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config cleanup.policy=delete

kafka-topics --create --if-not-exists \
  --bootstrap-server $KAFKA_BROKER \
  --topic events.add_to_cart \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config cleanup.policy=delete

kafka-topics --create --if-not-exists \
  --bootstrap-server $KAFKA_BROKER \
  --topic events.purchase \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config cleanup.policy=delete

kafka-topics --create --if-not-exists \
  --bootstrap-server $KAFKA_BROKER \
  --topic events.abandonment \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config compression.type=snappy \
  --config cleanup.policy=delete

# Dead Letter Queue - for failed validations
kafka-topics --create --if-not-exists \
  --bootstrap-server $KAFKA_BROKER \
  --topic events.dlq \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=2592000000 \
  --config compression.type=snappy \
  --config cleanup.policy=delete

echo "Listing all topics:"
kafka-topics --list --bootstrap-server $KAFKA_BROKER

echo "Topic configurations:"
kafka-topics --describe --bootstrap-server $KAFKA_BROKER

echo "Kafka topic initialization completed!"

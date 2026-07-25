"""
Spark Structured Streaming Job #1: Raw Event Writer
Reads events from Kafka and writes to MinIO (S3-compatible storage) as Parquet files

WHY THIS EXISTS:
- Creates immutable raw data lake for audit trail and reprocessing
- Parquet format provides compression and efficient columnar storage
- Partitioned by date and event type for fast queries

WHAT BREAKS WITHOUT IT:
- No raw data backup, can't reprocess if downstream logic changes
- No compliance/audit trail
- Can't backfill if processed data is corrupted

REAL-WORLD USAGE:
- Netflix, Uber store all raw events in S3 for reprocessing
- Data lake is foundation of Lambda architecture
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_extract, year, month, dayofmonth, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType
import os

# MinIO Configuration (S3-compatible)
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'raw-events')

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise RuntimeError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY environment variables must be set")

# Kafka Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:19092')
CHECKPOINT_LOCATION = "/tmp/checkpoints/raw-event-writer"

# Event topics to consume
EVENT_TOPICS = [
    'events.pageview',
    'events.product_click',
    'events.add_to_cart',
    'events.purchase',
    'events.abandonment'
]


def create_spark_session():
    """Initialize Spark session with S3/MinIO configuration"""
    return SparkSession.builder \
        .appName("StreamMart-RawEventWriter") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.minBatchesToRetain", "10") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def read_kafka_stream(spark, topic):
    """Read streaming data from Kafka topic"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()


def parse_event_value(df, event_type):
    """
    Parse Avro-serialized event value from Kafka
    
    Note: In production, use confluent-avro deserializer
    For this demo, we'll parse the JSON representation
    """
    # Common fields across all events
    base_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("timestamp", LongType(), False),
        StructField("device_type", StringType(), False),
    ])
    
    # Event-specific schemas
    event_schemas = {
        'pageview': base_schema.add("page_url", StringType()) \
                                .add("page_type", StringType()) \
                                .add("referrer", StringType()) \
                                .add("user_agent", StringType()) \
                                .add("ip_address", StringType()),
        
        'product_click': base_schema.add("product_id", StringType()) \
                                    .add("product_name", StringType()) \
                                    .add("product_category", StringType()) \
                                    .add("product_price", DoubleType()) \
                                    .add("click_position", IntegerType()) \
                                    .add("list_type", StringType()),
        
        'add_to_cart': base_schema.add("product_id", StringType()) \
                                   .add("product_name", StringType()) \
                                   .add("product_category", StringType()) \
                                   .add("product_price", DoubleType()) \
                                   .add("quantity", IntegerType()) \
                                   .add("cart_total", DoubleType()) \
                                   .add("cart_item_count", IntegerType()),
        
        'purchase': base_schema.add("order_id", StringType()) \
                               .add("items", StringType()) \
                               .add("subtotal", DoubleType()) \
                               .add("tax", DoubleType()) \
                               .add("shipping", DoubleType()) \
                               .add("total", DoubleType()) \
                               .add("payment_method", StringType()) \
                               .add("shipping_address", StringType()),
        
        'abandonment': base_schema.add("cart_items", StringType()) \
                                  .add("cart_total", DoubleType()) \
                                  .add("abandonment_stage", StringType()) \
                                  .add("session_duration_seconds", IntegerType()),
    }
    
    schema = event_schemas.get(event_type, base_schema)
    
    # Parse JSON value
    return df.select(
        col("key").cast("string").alias("partition_key"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset")
    ).select(
        "data.*",
        "kafka_timestamp",
        "kafka_partition",
        "kafka_offset",
        lit(event_type).alias("event_type")
    )


def write_to_minio(df, event_type):
    """
    Write stream to MinIO with partitioning
    
    Partitioning strategy:
    - By date (year/month/day) for time-range queries
    - By event_type for filtering by event
    - Snappy compression for storage efficiency
    
    This enables efficient queries like:
    - "Get all purchases for 2026-02-26"
    - "Get all events for specific session"
    """
    
    # Add date partitioning columns
    df_with_partitions = df \
        .withColumn("event_timestamp", (col("timestamp") / 1000).cast("timestamp")) \
        .withColumn("year", year("event_timestamp")) \
        .withColumn("month", month("event_timestamp")) \
        .withColumn("day", dayofmonth("event_timestamp"))
    
    # Write to MinIO/S3
    output_path = f"s3a://{MINIO_BUCKET}/events"
    
    query = df_with_partitions.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/{event_type}") \
        .partitionBy("year", "month", "day", "event_type") \
        .outputMode("append") \
        .start()
    
    return query


def main():
    """
    Main execution
    
    This job runs continuously, reading from all Kafka topics in a single stream
    and writing to MinIO with proper partitioning
    It's fault-tolerant with checkpointing - can resume from last processed offset
    """
    print("=" * 80)
    print("StreamMart Raw Event Writer - Starting...")
    print("=" * 80)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Subscribe to all event topics in a single stream using pattern
    topic_pattern = "events\\.(pageview|product_click|add_to_cart|purchase|abandonment)"
    
    print(f"Subscribing to topic pattern: {topic_pattern}")
    
    # Read from all Kafka topics at once
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribePattern", topic_pattern) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Extract event_type from topic name and parse JSON value
    # Schema for parsing common event fields (all events have these)
    common_event_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("timestamp", LongType(), False),
        StructField("device_type", StringType(), False)
    ])
    
    parsed_df = kafka_df \
        .withColumn("topic", col("topic").cast("string")) \
        .withColumn("event_type", regexp_replace(col("topic"), "events\\.", "")) \
        .withColumn("value_str", col("value").cast("string")) \
        .withColumn("event_data", from_json(col("value_str"), common_event_schema)) \
        .select(
            col("key").cast("string").alias("session_id"),
            col("value_str"),
            col("timestamp").alias("kafka_timestamp"),
            col("event_type"),
            col("event_data.timestamp").alias("event_timestamp_ms")
        ) \
        .filter(col("event_timestamp_ms").isNotNull()) \
        .withColumn("event_timestamp", (col("event_timestamp_ms") / 1000).cast("timestamp")) \
        .withColumn("year", year("event_timestamp")) \
        .withColumn("month", month("event_timestamp")) \
        .withColumn("day", dayofmonth("event_timestamp"))
    
    # Write to MinIO/S3 as Parquet, partitioned by date and event type
    output_path = f"s3a://{MINIO_BUCKET}/events"
    
    print(f"Writing to: {output_path}")
    print(f"Checkpoint: {CHECKPOINT_LOCATION}")
    print("✓ Stream started - processing all event types in unified stream")
    
    query = parsed_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .partitionBy("year", "month", "day", "event_type") \
        .outputMode("append") \
        .start()
    
    print("\n" + "=" * 80)
    print(f"Stream running. Writing to s3a://{MINIO_BUCKET}/events")
    print("Press Ctrl+C to stop...")
    print("=" * 80 + "\n")
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping stream...")
        query.stop()
        spark.stop()
        print("Raw Event Writer stopped")


if __name__ == "__main__":
    main()

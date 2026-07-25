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

DESIGN NOTE — why we store the raw JSON string, not exploded columns:
This is the bronze/raw layer. We deliberately keep the full event payload as an
opaque JSON string (`value_str`) rather than parsing it into per-event-type typed
columns. That means a producer-side field addition/removal never breaks this job
(schema-on-read), and reprocessing downstream logic can always re-derive anything
from the untouched original payload. Typed parsing happens in the Spark jobs that
actually need specific fields (window_aggregator, session_tracker, revenue_aggregator).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, regexp_replace, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, LongType
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

TOPIC_PATTERN = "events\\.(pageview|product_click|add_to_cart|purchase|abandonment)"

# Minimal schema used only to pull the event timestamp for date partitioning.
# The full payload is preserved untouched in `value_str` — see module docstring.
COMMON_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("session_id", StringType(), False),
    StructField("timestamp", LongType(), False),
    StructField("device_type", StringType(), False)
])


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


def read_kafka_stream(spark):
    """Subscribe to all five event topics via a single pattern-based stream."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribePattern", TOPIC_PATTERN) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()


def parse_events(kafka_df):
    """
    Extract event_type from the topic name, keep the raw JSON payload verbatim,
    and derive year/month/day partition columns from the event's own timestamp
    (not Kafka's ingestion timestamp — see design note in the module docstring).
    """
    return kafka_df \
        .withColumn("topic", col("topic").cast("string")) \
        .withColumn("event_type", regexp_replace(col("topic"), "events\\.", "")) \
        .withColumn("value_str", col("value").cast("string")) \
        .withColumn("event_data", from_json(col("value_str"), COMMON_EVENT_SCHEMA)) \
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


def write_to_minio(parsed_df):
    """Write the parsed stream to MinIO as Parquet, partitioned by date and event type."""
    output_path = f"s3a://{MINIO_BUCKET}/events"

    return parsed_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .partitionBy("year", "month", "day", "event_type") \
        .outputMode("append") \
        .start()


def main():
    """
    Main execution

    This job runs continuously, reading from all Kafka topics in a single stream
    and writing to MinIO with proper partitioning. It's fault-tolerant with
    checkpointing - can resume from last processed offset.
    """
    print("=" * 80)
    print("StreamMart Raw Event Writer - Starting...")
    print("=" * 80)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Subscribing to topic pattern: {TOPIC_PATTERN}")
    kafka_df = read_kafka_stream(spark)

    print("Parsing events...")
    parsed_df = parse_events(kafka_df)

    output_path = f"s3a://{MINIO_BUCKET}/events"
    print(f"Writing to: {output_path}")
    print(f"Checkpoint: {CHECKPOINT_LOCATION}")

    query = write_to_minio(parsed_df)
    print("✓ Stream started - processing all event types in unified stream")

    print("\n" + "=" * 80)
    print(f"Stream running. Writing to {output_path}")
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

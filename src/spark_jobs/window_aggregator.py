"""
Spark Structured Streaming Job #2: Window Aggregator
Computes 1-minute windowed aggregations and writes to PostgreSQL

WHY THIS EXISTS:
- Real-time metrics for dashboards (event counts, unique users, etc.)
- Windowed aggregations reduce data volume (millions of events → thousands of metrics)
- Enables trending and anomaly detection

WHAT BREAKS WITHOUT IT:
- Can't build real-time dashboards
- Must query raw events (slow)
- No real-time alerting on traffic spikes/drops

REAL-WORLD USAGE:
- Twitter uses windowed aggregations for trending topics
- Uber uses them for surge pricing calculations
- Netflix for real-time viewership metrics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, from_json, count, approx_count_distinct,
    window, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, LongType
import os

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:19092')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'streammart')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'streammart_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')

if not POSTGRES_PASSWORD:
    raise RuntimeError("POSTGRES_PASSWORD environment variable is not set")

CHECKPOINT_LOCATION = "/tmp/checkpoints/window-aggregator"

EVENT_TOPICS = [
    'events.pageview',
    'events.product_click',
    'events.add_to_cart',
    'events.purchase',
    'events.abandonment'
]


def create_spark_session():
    """Initialize Spark session with PostgreSQL JDBC driver"""
    return SparkSession.builder \
        .appName("StreamMart-WindowAggregator") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.sql.streaming.stateStore.providerClass", \
                "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.minBatchesToRetain", "10") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def read_all_kafka_topics(spark):
    """
    Read from all event topics into a single stream
    
    This allows unified windowing across all event types
    """
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", ",".join(EVENT_TOPICS)) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "10000") \
        .load()


def parse_events(df):
    """
    Extract key fields from Kafka messages
    
    We only need event_id, session_id, user_id, timestamp, and topic
    for aggregations - don't need full event payload
    """
    # Extract event_type from topic name
    df_with_type = df.withColumn(
        "event_type",
        regexp_extract(col("topic"), "events\\.(.*)", 1)
    )
    
    # Parse minimal fields from JSON
    minimal_schema = StructType([
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("user_id", StringType()),
        StructField("timestamp", LongType())
    ])
    
    return df_with_type.select(
        from_json(col("value").cast("string"), minimal_schema).alias("data"),
        col("event_type"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        col("data.event_id"),
        col("data.session_id"),
        col("data.user_id"),
        (col("data.timestamp") / 1000).cast("timestamp").alias("event_time"),
        col("event_type")
    ).filter(col("event_time").isNotNull())


def compute_1min_aggregations(df):
    """
    Compute 1-minute windowed aggregations
    
    Metrics:
    - Event count per type
    - Unique sessions per type
    - Unique users per type
    
    Watermarking:
    - Allows 2 minutes of late data
    - Events arriving > 2 min late are dropped
    - Prevents unbounded state growth
    
    Why watermarking: In production, events can arrive out-of-order due to:
    - Network delays
    - Client-side buffering
    - Kafka partition rebalancing
    """
    return df \
        .withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("event_type")
        ) \
        .agg(
            count("event_id").alias("count"),
            approx_count_distinct("session_id").alias("unique_sessions"),
            approx_count_distinct("user_id").alias("unique_users")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("count"),
            col("unique_sessions"),
            col("unique_users"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )


def write_to_postgres(df, table_name):
    """
    Write aggregations to PostgreSQL with upsert logic
    
    CRITICAL: Must be idempotent!
    - Uses ON CONFLICT to handle duplicates
    - If same window arrives twice, we UPDATE instead of failing
    
    Why this matters:
    - Spark may reprocess data after failures
    - Exactly-once semantics requires idempotent writes
    """
    
    postgres_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    def write_batch_to_postgres(batch_df, batch_id):
        """
        Custom writer with UPSERT logic using psycopg2.

        Uses ON CONFLICT (window_start, event_type) DO UPDATE so that
        Spark restarts and reprocessed windows never cause UniqueViolation.
        .collect() is safe here: max 5 rows per batch (one per event type).
        """
        if batch_df.rdd.isEmpty():
            return

        import psycopg2
        rows = batch_df.collect()
        print(f"Writing batch {batch_id} to PostgreSQL ({len(rows)} rows)")

        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=int(POSTGRES_PORT),
            dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO metrics_1min
                        (window_start, window_end, event_type, count,
                         unique_sessions, unique_users, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (window_start, event_type) DO UPDATE SET
                        count            = EXCLUDED.count,
                        unique_sessions  = EXCLUDED.unique_sessions,
                        unique_users     = EXCLUDED.unique_users,
                        updated_at       = EXCLUDED.updated_at
                    """,
                    [
                        (r.window_start, r.window_end, r.event_type, r.count,
                         r.unique_sessions, r.unique_users, r.created_at, r.updated_at)
                        for r in rows
                    ]
                )
            conn.commit()
            print(f"✓ Batch {batch_id} written successfully")
        finally:
            conn.close()
    
    # Use foreachBatch for custom write logic
    return df.writeStream \
        .foreachBatch(write_batch_to_postgres) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()


def main():
    """
    Main execution
    
    This job:
    1. Reads all events from Kafka
    2. Computes 1-min windowed aggregations
    3. Writes to PostgreSQL with upserts
    4. Processes micro-batches every 30 seconds
    """
    print("=" * 80)
    print("StreamMart Window Aggregator - Starting...")
    print("=" * 80)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Read from Kafka
    print("Reading from Kafka topics...")
    kafka_df = read_all_kafka_topics(spark)
    
    # Parse events
    print("Parsing events...")
    events_df = parse_events(kafka_df)
    
    # Compute 1-minute aggregations
    print("Computing 1-minute windowed aggregations...")
    aggregations_df = compute_1min_aggregations(events_df)
    
    # Write to PostgreSQL
    print(f"Writing to PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    query = write_to_postgres(aggregations_df, "metrics_1min")
    
    print("\n" + "=" * 80)
    print("Window Aggregator running. Metrics -> PostgreSQL @ 30sec intervals")
    print("Press Ctrl+C to stop...")
    print("=" * 80 + "\n")
    
    # Monitor query progress
    try:
        while query.isActive:
            import time
            time.sleep(10)
            
            # Print progress
            progress = query.lastProgress
            if progress:
                print(f"[Progress] Processed {progress['numInputRows']} rows, "
                      f"Batch: {progress['batchId']}")
        
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStopping aggregator...")
        query.stop()
        spark.stop()
        print("Window Aggregator stopped")


if __name__ == "__main__":
    main()

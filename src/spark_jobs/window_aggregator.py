"""
Spark Structured Streaming Job #2: Window Aggregator
Computes 1-minute AND 5-minute windowed aggregations and writes both to PostgreSQL

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

WHY THIS JOB OWNS BOTH metrics_1min AND metrics_5min:
An earlier version derived metrics_5min from metrics_1min in a periodic SQL
rollup (SUM(unique_sessions) across five 1-minute rows). That is mathematically
wrong: unique_sessions/unique_users are approximate DISTINCT counts
(approx_count_distinct), and summing pre-aggregated distinct counts is not the
same as counting distinct values over the union — a session active across
three consecutive 1-minute windows would be counted three times. There is no
way to correctly recover a 5-minute distinct count from 1-minute pre-aggregated
distinct counts after the fact; the only correct fix is to compute the
5-minute distinct count directly from the raw event stream, same as the
1-minute one. That's what this job now does: two independent windowed
aggregations off the same parsed event stream, each with its own watermark,
checkpoint, and upsert target.
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


def compute_windowed_aggregations(df, window_duration, watermark_delay):
    """
    Compute windowed aggregations for an arbitrary tumbling window size.

    Metrics:
    - Event count per type
    - Unique sessions per type (approx_count_distinct — see module docstring
      for why this must never be summed across windows after the fact)
    - Unique users per type

    Why watermarking: In production, events can arrive out-of-order due to:
    - Network delays
    - Client-side buffering
    - Kafka partition rebalancing
    """
    return df \
        .withWatermark("event_time", watermark_delay) \
        .groupBy(
            window(col("event_time"), window_duration),
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


def write_to_postgres(df, table_name, checkpoint_suffix, trigger_interval):
    """
    Write windowed aggregations to PostgreSQL with upsert logic

    CRITICAL: Must be idempotent!
    - Uses ON CONFLICT to handle duplicates
    - If same window arrives twice, we UPDATE instead of failing

    Why this matters:
    - Spark may reprocess data after failures
    - Exactly-once semantics requires idempotent writes
    """

    checkpoint_location = f"{CHECKPOINT_LOCATION}/{checkpoint_suffix}"

    def write_batch_to_postgres(batch_df, batch_id):
        """
        Custom writer with UPSERT logic using psycopg2.

        Uses ON CONFLICT (window_start, event_type) DO UPDATE so that
        Spark restarts and reprocessed windows never cause UniqueViolation.
        .collect() is safe here: at most 5 rows per batch (one per event type).
        """
        if batch_df.rdd.isEmpty():
            return

        import psycopg2
        rows = batch_df.collect()
        print(f"[{table_name}] Writing batch {batch_id} to PostgreSQL ({len(rows)} rows)")

        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=int(POSTGRES_PORT),
            dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    f"""
                    INSERT INTO {table_name}
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
            print(f"✓ [{table_name}] Batch {batch_id} written successfully")
        finally:
            conn.close()

    # Use foreachBatch for custom write logic
    return df.writeStream \
        .foreachBatch(write_batch_to_postgres) \
        .option("checkpointLocation", checkpoint_location) \
        .outputMode("update") \
        .trigger(processingTime=trigger_interval) \
        .start()


def main():
    """
    Main execution

    This job:
    1. Reads all events from Kafka
    2. Computes 1-min windowed aggregations -> metrics_1min
    3. Computes 5-min windowed aggregations -> metrics_5min (independently,
       not derived from metrics_1min — see module docstring)
    4. Writes both to PostgreSQL with upserts, each on its own trigger interval
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
    agg_1min_df = compute_windowed_aggregations(events_df, "1 minute", "2 minutes")

    # Compute 5-minute aggregations (independent aggregation, not a rollup of 1-min)
    print("Computing 5-minute windowed aggregations...")
    agg_5min_df = compute_windowed_aggregations(events_df, "5 minutes", "2 minutes")

    # Write both to PostgreSQL
    print(f"Writing to PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    query_1min = write_to_postgres(agg_1min_df, "metrics_1min", "1min", "30 seconds")
    query_5min = write_to_postgres(agg_5min_df, "metrics_5min", "5min", "60 seconds")

    print("\n" + "=" * 80)
    print("Window Aggregator running.")
    print("  metrics_1min -> PostgreSQL @ 30sec intervals")
    print("  metrics_5min -> PostgreSQL @ 60sec intervals")
    print("Press Ctrl+C to stop...")
    print("=" * 80 + "\n")

    queries = [query_1min, query_5min]

    # Monitor query progress
    try:
        while any(q.isActive for q in queries):
            import time
            time.sleep(10)

            for q in queries:
                progress = q.lastProgress
                if progress:
                    print(f"[Progress:{q.name or q.id}] Processed {progress['numInputRows']} rows, "
                          f"Batch: {progress['batchId']}")

        for q in queries:
            q.awaitTermination()

    except KeyboardInterrupt:
        print("\nStopping aggregator...")
        for q in queries:
            q.stop()
        spark.stop()
        print("Window Aggregator stopped")


if __name__ == "__main__":
    main()

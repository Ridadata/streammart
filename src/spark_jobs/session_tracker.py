"""
Spark Structured Streaming Job #3: Session Tracker
Sessionizes user behavior and computes per-session KPIs

WHY THIS EXISTS:
- Converts raw events into session-level insights
- Tracks conversion: did user browse → add to cart → purchase?
- Enables cohort analysis and user journey optimization

WHAT BREAKS WITHOUT IT:
- Can't calculate conversion rates
- Can't identify drop-off points in funnel
- Can't personalize user experience

REAL-WORLD USAGE:
- Google Analytics sessionization for user behavior analysis
- Amazon tracks sessions to optimize checkout flow
- Spotify uses sessions to understand listening patterns
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_extract, from_json, coalesce, lit,
    first, count, sum, max, when,
    session_window, unix_timestamp, round, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql import Window
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

CHECKPOINT_LOCATION = "/tmp/checkpoints/session-tracker"

EVENT_TOPICS = [
    'events.pageview',
    'events.product_click',
    'events.add_to_cart',
    'events.purchase',
    'events.abandonment'
]


def create_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("StreamMart-SessionTracker") \
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


def read_kafka_events(spark):
    """Read all event types from Kafka"""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", ",".join(EVENT_TOPICS)) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()


def parse_events(df):
    """
    Parse events with session-relevant fields
    
    For sessionization, we need:
    - session_id (grouping key)
    - event_type (to count different actions)
    - timestamp (to find session start/end)
    - revenue (for purchases)
    """
    
    # Define schema for parsing
    event_schema = StructType([
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("user_id", StringType()),
        StructField("timestamp", LongType()),
        StructField("device_type", StringType()),
        # Purchase-specific fields
        StructField("total", DoubleType()),
        StructField("order_id", StringType())
    ])
    
    return df.select(
        regexp_extract(col("topic"), "events\\.(.*)", 1).alias("event_type"),
        from_json(col("value").cast("string"), event_schema).alias("data")
    ).select(
        col("data.session_id"),
        col("data.user_id"),
        col("event_type"),
        (col("data.timestamp") / 1000).cast("timestamp").alias("event_time"),
        col("data.device_type"),
        coalesce(col("data.total"), lit(0.0)).alias("revenue")
    ).filter(col("session_id").isNotNull())


def sessionize_events(df):
    """
    Compute session-level aggregations
    
    Session timeout: 30 minutes of inactivity = session ends
    
    Metrics per session:
    - Start/end time
    - Duration
    - Event counts by type
    - Converted (true if purchase exists)
    - Total revenue
    - Device type
    
    This is a stateful aggregation - Spark maintains session state
    across micro-batches using state store
    """
    return df \
        .withWatermark("event_time", "1 hour") \
        .groupBy(
            session_window(col("event_time"), "30 minutes"),
            col("session_id")
        ) \
        .agg(
            first("user_id").alias("user_id"),
            first("device_type").alias("device_type"),
            count("*").alias("total_events"),
            sum(when(col("event_type") == "pageview", 1).otherwise(0)).alias("pageview_count"),
            sum(when(col("event_type") == "product_click", 1).otherwise(0)).alias("product_click_count"),
            sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_count"),
            sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
            max(when(col("event_type") == "purchase", lit(True)).otherwise(lit(False))).alias("converted"),
            sum(col("revenue")).alias("revenue")
        ) \
        .select(
            col("session_id"),
            col("user_id"),
            col("session_window.start").alias("start_time"),
            col("session_window.end").alias("end_time"),
            (unix_timestamp(col("session_window.end")) - unix_timestamp(col("session_window.start"))).cast("int").alias("duration_seconds"),
            col("total_events"),
            col("pageview_count"),
            col("product_click_count"),
            col("add_to_cart_count"),
            col("converted"),
            round(col("revenue"), 2).alias("revenue"),
            col("device_type"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )


def write_to_postgres(df, table_name):
    """
    Write session summaries to PostgreSQL with upsert
    
    ON CONFLICT (session_id) DO UPDATE
    - Updates existing sessions as new events arrive
    - Handles late-arriving data
    """
    
    postgres_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    def upsert_batch(batch_df, batch_id):
        """Write batch with upsert logic using psycopg2 ON CONFLICT DO UPDATE.

        session_summary has a UNIQUE index on session_id (see init_postgres.sql),
        so upsert is required on every Spark restart / reprocessing cycle.
        .collect() is safe here: batch size is bounded by active sessions in the
        current 1-minute trigger window.
        """
        if batch_df.rdd.isEmpty():
            return

        import psycopg2
        rows = batch_df.collect()
        print(f"[Batch {batch_id}] Writing {len(rows)} session rows to PostgreSQL")

        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=int(POSTGRES_PORT),
            dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO session_summary
                        (session_id, user_id, start_time, end_time, duration_seconds,
                         total_events, pageview_count, product_click_count, add_to_cart_count,
                         converted, revenue, device_type, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (session_id) DO UPDATE SET
                        end_time             = EXCLUDED.end_time,
                        duration_seconds     = EXCLUDED.duration_seconds,
                        total_events         = EXCLUDED.total_events,
                        pageview_count       = EXCLUDED.pageview_count,
                        product_click_count  = EXCLUDED.product_click_count,
                        add_to_cart_count    = EXCLUDED.add_to_cart_count,
                        converted            = EXCLUDED.converted,
                        revenue              = EXCLUDED.revenue,
                        updated_at           = EXCLUDED.updated_at
                    """,
                    [
                        (r.session_id, r.user_id, r.start_time, r.end_time,
                         r.duration_seconds, r.total_events, r.pageview_count,
                         r.product_click_count, r.add_to_cart_count, r.converted,
                         float(r.revenue), r.device_type, r.created_at, r.updated_at)
                        for r in rows
                    ]
                )
            conn.commit()
        finally:
            conn.close()
    
    return df.writeStream \
        .foreachBatch(upsert_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .outputMode("update") \
        .trigger(processingTime="1 minute") \
        .start()


def main():
    """
    Main execution
    
    Sessionization logic:
    1. Read all events from Kafka
    2. Group by session_id with 30-min timeout
    3. Compute per-session metrics
    4. Write to PostgreSQL (upsert on session_id)
    """
    print("=" * 80)
    print("StreamMart Session Tracker - Starting...")
    print("=" * 80)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Reading events from Kafka...")
    kafka_df = read_kafka_events(spark)
    
    print("Parsing events...")
    events_df = parse_events(kafka_df)
    
    print("Sessionizing events (30-min timeout)...")
    sessions_df = sessionize_events(events_df)
    
    print(f"Writing to PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    query = write_to_postgres(sessions_df, "session_summary")
    
    print("\n" + "=" * 80)
    print("Session Tracker running. Sessions -> PostgreSQL @ 1min intervals")
    print("Session timeout: 30 minutes of inactivity")
    print("Press Ctrl+C to stop...")
    print("=" * 80 + "\n")
    
    # Monitor
    try:
        while query.isActive:
            import time
            time.sleep(15)
            
            progress = query.lastProgress
            if progress:
                print(f"[Progress] Batch {progress['batchId']}: "
                      f"{progress['numInputRows']} events processed")
        
        query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStopping session tracker...")
        query.stop()
        spark.stop()
        print("Session Tracker stopped")


if __name__ == "__main__":
    main()

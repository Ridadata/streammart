"""
Spark Structured Streaming Job #4: Revenue Aggregator
Computes real-time, per-product revenue metrics from purchase events

WHY THIS EXISTS:
- Business-critical metrics: revenue and units sold, per product per day
- Real-time visibility into sales performance
- Enables operational decisions (inventory, promotions, staffing)

WHAT BREAKS WITHOUT IT:
- No real-time revenue tracking
- Can't respond quickly to sales drops/spikes
- Manual aggregation from raw data (slower, error-prone)

REAL-WORLD USAGE:
- Shopify shows real-time GMV (Gross Merchandise Value) to merchants
- Amazon tracks revenue per minute for Prime Day monitoring
- Stripe provides real-time transaction volumes

SCOPE NOTE:
This job is the sole writer of `product_performance` (see ENGINEERING.md table-ownership
matrix). It intentionally does not also compute a separate hourly/global revenue
figure — `daily_revenue` is owned exclusively by the Airflow `daily_summary` DAG
(the authoritative end-of-day batch layer), and having this streaming job write to
it too would recreate the dual-writer bug this rewrite fixes. If a real-time
top-line revenue panel is needed later, it should read directly from
`product_performance` (SUM(revenue) across all products) rather than adding a
second writer to any table.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, sum, window,
    to_date, current_timestamp, round, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, IntegerType, ArrayType
)
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

CHECKPOINT_LOCATION = "/tmp/checkpoints/revenue-aggregator"


def create_spark_session():
    """Initialize Spark session"""
    return SparkSession.builder \
        .appName("StreamMart-RevenueAggregator") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()


def read_purchase_events(spark):
    """
    Read only purchase events from Kafka

    This job focuses on a single event type for specialized processing
    Demonstrates single-responsibility principle in streaming jobs
    """
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", "events.purchase") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()


def parse_purchase_events(df):
    """
    Extract revenue-relevant fields from purchase events, one row per line item.

    Fields needed:
    - timestamp (for time-based aggregations)
    - order_id (for order count, deduplication)
    - items (exploded — one row per product per order)
    """

    # Nested schema for purchase items
    item_schema = ArrayType(StructType([
        StructField("product_id", StringType()),
        StructField("product_name", StringType()),
        StructField("product_category", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DoubleType()),
        StructField("total_price", DoubleType())
    ]))

    purchase_schema = StructType([
        StructField("event_id", StringType()),
        StructField("session_id", StringType()),
        StructField("user_id", StringType()),
        StructField("timestamp", LongType()),
        StructField("order_id", StringType()),
        StructField("items", item_schema),
        StructField("subtotal", DoubleType()),
        StructField("tax", DoubleType()),
        StructField("shipping", DoubleType()),
        StructField("total", DoubleType()),
        StructField("payment_method", StringType())
    ])

    return df.select(
        from_json(col("value").cast("string"), purchase_schema).alias("data")
    ).select(
        col("data.order_id"),
        col("data.user_id"),
        (col("data.timestamp") / 1000).cast("timestamp").alias("purchase_time"),
        col("data.total").alias("order_total"),
        explode(col("data.items")).alias("item")
    ).select(
        col("order_id"),
        col("user_id"),
        col("purchase_time"),
        col("order_total"),
        col("item.product_id"),
        col("item.product_name"),
        col("item.product_category"),
        col("item.quantity"),
        col("item.unit_price"),
        col("item.total_price").alias("item_revenue")
    )


def compute_product_performance(df):
    """
    Compute product-level revenue per calendar day, updated continuously.

    Useful for:
    - Identifying best-sellers
    - Inventory planning
    - Recommendation systems

    BOUNDED STATE: the grouping key includes a real time window
    (window(purchase_time, "1 day")), not just a derived to_date() column.
    Spark's watermark-based state eviction only recognizes window columns
    produced by window()/session_window() in the grouping key — grouping by a
    plain date-derived column with a watermark on a different column does NOT
    bound state, and previously caused unbounded state growth here. With a
    real daily tumbling window, Spark evicts a day's state once the watermark
    passes that day's end, so state is bounded to ~1-2 days of distinct
    products at any time.

    Output mode: "update" — this is a time-window aggregation (not a session
    window), so update mode is fully supported and lets each row refresh
    throughout the day instead of only appearing once it's finalized.
    """

    return df \
        .withWatermark("purchase_time", "15 minutes") \
        .groupBy(
            window(col("purchase_time"), "1 day"),
            col("product_id"),
            col("product_name"),
            col("product_category")
        ) \
        .agg(
            sum("quantity").alias("units_sold"),
            sum("item_revenue").alias("revenue")
        ) \
        .select(
            col("product_id"),
            to_date(col("window.start")).alias("date"),
            col("product_name"),
            col("product_category"),
            lit(0).alias("view_count"),  # Filled by other jobs
            lit(0).alias("click_count"),  # Filled by other jobs
            lit(0).alias("add_to_cart_count"),  # Filled by other jobs
            col("units_sold").alias("purchase_count"),
            round(col("revenue"), 2).alias("revenue"),
            current_timestamp().alias("updated_at")
        )


def write_product_performance(df):
    """
    Write product_performance with a real upsert.

    product_performance has PRIMARY KEY (product_id, date) (see init_postgres.sql).
    The previous version of this job used JDBC .mode("append"), which cannot
    express ON CONFLICT and fails with a duplicate-key error the moment the
    same (product_id, date) is emitted twice in the same day (which happens on
    every trigger, by design, since this is an "update" mode aggregation).
    psycopg2 executemany + ON CONFLICT DO UPDATE mirrors the pattern already
    used in window_aggregator.py and session_tracker.py.

    Any exception here is intentionally NOT caught: foreachBatch exceptions
    fail the micro-batch and Spark retries it, which is the correct behavior
    for a write failure. The previous version caught all exceptions, printed
    a message, and returned normally — which silently dropped the batch while
    letting the stream's offset advance past it. That is data loss with no
    error surfaced anywhere, and must not be reintroduced.
    """

    def upsert_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        import psycopg2
        rows = batch_df.collect()
        print(f"[product_performance] Writing batch {batch_id} ({len(rows)} rows)")

        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=int(POSTGRES_PORT),
            dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD
        )
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO product_performance
                        (product_id, date, product_name, product_category,
                         view_count, click_count, add_to_cart_count,
                         purchase_count, revenue, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (product_id, date) DO UPDATE SET
                        product_name       = EXCLUDED.product_name,
                        product_category   = EXCLUDED.product_category,
                        purchase_count     = EXCLUDED.purchase_count,
                        revenue            = EXCLUDED.revenue,
                        updated_at         = EXCLUDED.updated_at
                    """,
                    [
                        (r.product_id, r.date, r.product_name, r.product_category,
                         r.view_count, r.click_count, r.add_to_cart_count,
                         r.purchase_count, float(r.revenue), r.updated_at)
                        for r in rows
                    ]
                )
            conn.commit()
            print(f"✓ [product_performance] Batch {batch_id} written successfully")
        finally:
            conn.close()

    return df.writeStream \
        .foreachBatch(upsert_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()


def main():
    """
    Main execution

    1. Read purchase events from Kafka
    2. Explode into per-line-item rows
    3. Aggregate into daily product-level performance (bounded state)
    4. Upsert into PostgreSQL product_performance
    """
    print("=" * 80)
    print("StreamMart Revenue Aggregator - Starting...")
    print("=" * 80)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading purchase events from Kafka...")
    kafka_df = read_purchase_events(spark)

    print("Parsing purchase events...")
    purchases_df = parse_purchase_events(kafka_df)

    print("Computing product performance metrics...")
    product_perf_df = compute_product_performance(purchases_df)

    print(f"\nWriting to PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    query = write_product_performance(product_perf_df)

    print("\n" + "=" * 80)
    print("Revenue Aggregator running. Metrics -> PostgreSQL @ 30sec intervals")
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
                      f"{progress['numInputRows']} purchase line items processed")

        query.awaitTermination()

    except KeyboardInterrupt:
        print("\nStopping revenue aggregator...")
        query.stop()
        spark.stop()
        print("Revenue Aggregator stopped")


if __name__ == "__main__":
    main()

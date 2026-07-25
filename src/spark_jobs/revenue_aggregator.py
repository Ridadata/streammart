"""
Spark Structured Streaming Job #4: Revenue Aggregator
Computes real-time revenue metrics from purchase events

WHY THIS EXISTS:
- Business-critical metrics: revenue, order count, AOV (Average Order Value)
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
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, explode, sum, count, avg, window,
    to_date, current_timestamp, round, lit, countDistinct
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
    Extract revenue-relevant fields from purchase events
    
    Fields needed:
    - timestamp (for time-based aggregations)
    - total (order total)
    - user_id (for unique customer count)
    - order_id (for order count, deduplication)
    - items (for product performance tracking)
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
        col("data.subtotal"),
        col("data.tax"),
        col("data.shipping"),
        col("data.payment_method"),
        explode(col("data.items")).alias("item")
    ).select(
        col("order_id"),
        col("user_id"),
        col("purchase_time"),
        col("order_total"),
        col("subtotal"),
        col("tax"),
        col("shipping"),
        col("payment_method"),
        col("item.product_id"),
        col("item.product_name"),
        col("item.product_category"),
        col("item.quantity"),
        col("item.unit_price"),
        col("item.total_price").alias("item_revenue")
    )


def compute_hourly_revenue(df):
    """
    Compute hourly revenue metrics
    
    Metrics:
    - Total revenue
    - Order count
    - Unique customers
    - Average order value (AOV)
    - Items sold
    
    Time window: 1 hour (tumbling window)
    Watermark: 15 minutes (handle late purchases)
    """
    
    return df \
        .withWatermark("purchase_time", "15 minutes") \
        .groupBy(
            window(col("purchase_time"), "1 hour")
        ) \
        .agg(
            sum("order_total").alias("total_revenue"),
            countDistinct("order_id").alias("order_count"),
            countDistinct("user_id").alias("unique_customers"),
            avg("order_total").alias("avg_order_value"),
            sum("quantity").alias("items_sold")
        ) \
        .select(
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            round(col("total_revenue"), 2).alias("total_revenue"),
            col("order_count"),
            col("unique_customers"),
            round(col("avg_order_value"), 2).alias("avg_order_value"),
            col("items_sold"),
            current_timestamp().alias("created_at")
        )


def compute_product_performance(df):
    """
    Compute product-level revenue (updated continuously)
    
    Useful for:
    - Identifying best-sellers
    - Inventory planning
    - Recommendation systems
    
    Aggregated by:
    - product_id
    - Date (for daily tracking)
    """
    
    return df \
        .withWatermark("purchase_time", "15 minutes") \
        .groupBy(
            col("product_id"),
            col("product_name"),
            col("product_category"),
            to_date(col("purchase_time")).alias("date")
        ) \
        .agg(
            sum("quantity").alias("units_sold"),
            sum("item_revenue").alias("revenue"),
            countDistinct("order_id").alias("order_count")
        ) \
        .select(
            col("product_id"),
            col("date"),
            col("product_name"),
            col("product_category"),
            lit(0).alias("view_count"),  # Filled by other jobs
            lit(0).alias("click_count"),  # Filled by other jobs
            lit(0).alias("add_to_cart_count"),  # Filled by other jobs
            col("units_sold").alias("purchase_count"),
            round(col("revenue"), 2).alias("revenue"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )


def write_to_postgres_table(df, table_name, key_columns):
    """
    Generic PostgreSQL writer with upsert
    
    Args:
        df: DataFrame to write
        table_name: Target table
        key_columns: List of columns that form the primary key (for upsert)
    """
    
    postgres_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    
    def upsert_batch(batch_df, batch_id):
        if batch_df.count() == 0:
            return
        
        print(f"[{table_name}] Writing batch {batch_id} ({batch_df.count()} rows)")
        
        try:
            # Simple append - relies on PostgreSQL constraints for deduplication
            # In production, implement proper UPSERT with ON CONFLICT
            batch_df.write \
                .format("jdbc") \
                .option("url", postgres_url) \
                .option("dbtable", table_name) \
                .option("user", POSTGRES_USER) \
                .option("password", POSTGRES_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            print(f"✓ [{table_name}] Batch {batch_id} written")
            
        except Exception as e:
            print(f"✗ [{table_name}] Error writing batch: {e}")
    
    return df.writeStream \
        .foreachBatch(upsert_batch) \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/{table_name}") \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()


def main():
    """
    Main execution
    
    Runs two parallel aggregations:
    1. Hourly revenue metrics
    2. Product-level performance
    
    Both write to PostgreSQL for dashboard consumption
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
    
    # Cache for reuse in multiple aggregations
    purchases_df.persist()
    
    print("Computing hourly revenue aggregations...")
    hourly_revenue_df = compute_hourly_revenue(purchases_df)
    
    print("Computing product performance metrics...")
    product_perf_df = compute_product_performance(purchases_df)
    
    print(f"\nWriting to PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    
    # Start both streaming queries
    # Note: In production, consider separate jobs for better isolation
    
    # Query 1: Hourly revenue (not using for this demo, but shows the pattern)
    # query1 = write_to_postgres_table(hourly_revenue_df, "hourly_revenue", ["hour_start"])
    
    # Query 2: Product performance
    query2 = write_to_postgres_table(product_perf_df, "product_performance", ["product_id", "date"])
    
    print("\n" + "=" * 80)
    print("Revenue Aggregator running. Metrics -> PostgreSQL @ 30sec intervals")
    print("Press Ctrl+C to stop...")
    print("=" * 80 + "\n")
    
    # Monitor
    try:
        queries = [query2]
        
        for query in queries:
            while query.isActive:
                import time
                time.sleep(15)
                
                progress = query.lastProgress
                if progress:
                    print(f"[Progress] Batch {progress['batchId']}: "
                          f"{progress['numInputRows']} purchases processed")
        
        for query in queries:
            query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\nStopping revenue aggregator...")
        for query in [query2]:
            query.stop()
        spark.stop()
        print("Revenue Aggregator stopped")


if __name__ == "__main__":
    main()

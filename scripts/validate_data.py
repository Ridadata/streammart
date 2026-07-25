"""
Data Validation Script
Checks data quality and pipeline health

Run from repo root (needs POSTGRES_*, MINIO_* env vars set, e.g. via
`.env` + `python-dotenv`, or exported in your shell):

    python scripts/validate_data.py
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'utils'))

from postgres_client import get_postgres_client
from minio_client import get_minio_client
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Real tables per sql/init_postgres.sql. There is deliberately no `events_raw`
# table — raw events live in MinIO as Parquet (see src/spark_jobs/raw_event_writer.py),
# not in Postgres. An earlier version of this script checked for events_raw and
# had a syntax error that meant it never ran at all.
REQUIRED_TABLES = [
    'metrics_1min',
    'metrics_5min',
    'session_summary',
    'product_performance',
    'daily_revenue',
    'daily_summary',
    'product_daily_performance',
    'data_quality_checks',
    'pipeline_monitoring',
]


def check_database_tables():
    """Verify all required tables exist"""
    logger.info("Checking database tables...")

    client = get_postgres_client()

    missing_tables = []
    for table in REQUIRED_TABLES:
        if not client.table_exists(table):
            missing_tables.append(table)
            logger.error(f"✗ Table missing: {table}")
        else:
            row_count = client.get_table_row_count(table)
            logger.info(f"✓ {table}: {row_count} rows")

    if missing_tables:
        logger.error(f"Missing tables: {', '.join(missing_tables)}")
        return False

    logger.info("✓ All required tables exist")
    return True


def check_event_data():
    """
    Check if events are being ingested, via metrics_1min — the finest-grained
    per-event-type activity visible in Postgres. (Raw per-event data lives in
    MinIO; see check_minio_storage() below for checking that directly.)
    """
    logger.info("\nChecking event data...")

    client = get_postgres_client()

    total_windows = client.get_table_row_count('metrics_1min')
    logger.info(f"Total metrics_1min windows recorded: {total_windows}")

    if total_windows == 0:
        logger.warning("⚠ No windows recorded yet. Is window_aggregator.py running?")
        return False

    recent = client.execute_query("""
        SELECT COALESCE(SUM(count), 0) as total_events
        FROM metrics_1min
        WHERE window_start >= NOW() - INTERVAL '10 minutes'
    """)
    recent_count = recent[0]['total_events'] if recent else 0
    logger.info(f"Events in last 10 minutes: {recent_count}")

    if recent_count == 0:
        logger.warning("⚠ No recent events. Event generator or Spark jobs may be stopped.")
        return False

    event_types = client.execute_query("""
        SELECT event_type, SUM(count) as total_events
        FROM metrics_1min
        WHERE window_start >= NOW() - INTERVAL '1 hour'
        GROUP BY event_type
        ORDER BY total_events DESC
    """)

    logger.info("\nEvent type distribution (last hour):")
    for row in event_types:
        logger.info(f"  {row['event_type']}: {row['total_events']}")

    logger.info("✓ Event data looks good")
    return True


def check_aggregations():
    """Check if session and product aggregations are being computed"""
    logger.info("\nChecking aggregations...")

    client = get_postgres_client()

    recent_metrics = client.execute_query("""
        SELECT COUNT(*) as count
        FROM metrics_1min
        WHERE window_start >= NOW() - INTERVAL '1 hour'
    """)
    metrics_count = recent_metrics[0]['count'] if recent_metrics else 0
    logger.info(f"1-min metric windows in last hour: {metrics_count}")

    if metrics_count == 0:
        logger.warning("⚠ No recent metrics. Is window_aggregator.py running?")
        return False

    session_count = client.get_table_row_count('session_summary')
    logger.info(f"Total sessions tracked: {session_count}")

    # session_tracker.py runs in append output mode (required for
    # session_window aggregations), so a session only appears here roughly
    # 40-70 minutes after its last event. See CLAUDE.md / session_tracker.py
    # for why. A recent-window check here would false-positive by design,
    # so we only report total volume rather than a "last N minutes" count.

    product_count = client.get_table_row_count('product_performance')
    logger.info(f"Total product-performance rows tracked: {product_count}")

    logger.info("✓ Aggregations are being computed")
    return True


def check_minio_storage():
    """Check MinIO object storage for the raw event data lake"""
    logger.info("\nChecking MinIO storage...")

    try:
        client = get_minio_client()

        bucket_name = os.getenv('MINIO_BUCKET', 'raw-events')
        if not client.bucket_exists(bucket_name):
            logger.error(f"✗ Bucket '{bucket_name}' does not exist")
            return False

        objects = client.list_objects(bucket_name, prefix='events/')
        object_count = len(objects)

        logger.info(f"Objects in {bucket_name}: {object_count}")

        if object_count > 0:
            logger.info("\nSample objects:")
            for obj in objects[:5]:
                logger.info(f"  {obj}")
        else:
            logger.warning(
                "⚠ No objects found yet. Is raw_event_writer.py running? "
                "(it writes to s3a://<bucket>/events/, checkpointed and "
                "append-only, so this should never stay empty for long "
                "once purchase/pageview traffic is flowing)"
            )

        total_size = client.get_bucket_size(bucket_name)
        size_mb = total_size / (1024 * 1024)
        logger.info(f"Total storage: {size_mb:.2f} MB")

        logger.info("✓ MinIO storage is operational")
        return True

    except Exception as e:
        logger.error(f"✗ MinIO check failed: {e}")
        return False


def check_data_quality():
    """Check data quality metrics"""
    logger.info("\nChecking data quality...")

    client = get_postgres_client()

    dq_checks = client.execute_query("""
        SELECT
            check_name,
            check_type,
            passed,
            check_timestamp
        FROM data_quality_checks
        ORDER BY check_timestamp DESC
        LIMIT 10
    """)

    if not dq_checks:
        logger.warning(
            "⚠ No data quality checks found. Is the postgres-maintenance "
            "container running (heartbeat checks) or has the Airflow "
            "streammart_data_quality DAG run yet (business-rule checks)?"
        )
        return False

    logger.info("\nRecent DQ checks:")
    for check in dq_checks:
        status = "✓ PASS" if check['passed'] else "✗ FAIL"
        logger.info(f"  {check['check_name']} ({check['check_type']}): {status}")

    failures = sum(1 for check in dq_checks if not check['passed'])
    if failures > 0:
        logger.warning(f"⚠ {failures} quality checks failed")
    else:
        logger.info("✓ All quality checks passed")

    return True


def main():
    """Run all validation checks"""
    logger.info("=" * 60)
    logger.info("StreamMart Data Validation")
    logger.info("=" * 60 + "\n")

    checks = [
        ("Database Tables", check_database_tables),
        ("Event Data", check_event_data),
        ("Aggregations", check_aggregations),
        ("MinIO Storage", check_minio_storage),
        ("Data Quality", check_data_quality),
    ]

    results = {}
    for check_name, check_func in checks:
        try:
            results[check_name] = check_func()
        except Exception as e:
            logger.error(f"✗ {check_name} check failed with error: {e}")
            results[check_name] = False

    logger.info("\n" + "=" * 60)
    logger.info("Validation Summary")
    logger.info("=" * 60)

    for check_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        logger.info(f"{check_name}: {status}")

    all_passed = all(results.values())

    logger.info("\n" + "=" * 60)
    if all_passed:
        logger.info("All validation checks passed! ✓")
    else:
        logger.warning("Some validation checks failed. Review logs above.")
    logger.info("=" * 60 + "\n")

    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())

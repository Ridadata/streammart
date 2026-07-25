"""
Data Validation Script
Checks data quality and pipeline health
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'utils'))

from postgres_client import get_postgres_client
from minio_client import get_minio_client
from datetime import datetime, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger __name__)


def check_database_tables():
    """Verify all required tables exist"""
    logger.info("Checking database tables...")
    
    client = get_postgres_client()
    
    required_tables = [
        'events_raw',
        'metrics_1min',
        'metrics_5min',
        'session_summary',
        'product_performance',
        'daily_revenue',
        'data_quality_checks',
        'pipeline_monitoring'
    ]
    
    missing_tables = []
    for table in required_tables:
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
    """Check if events are being ingested"""
    logger.info("\nChecking event data...")
    
    client = get_postgres_client()
    
    # Check total events
    total_events = client.get_table_row_count('events_raw')
    logger.info(f"Total events in database: {total_events}")
    
    if total_events == 0:
        logger.warning("⚠ No events found. Is the event generator running?")
        return False
    
    # Check recent events
    recent_events = client.execute_query("""
        SELECT COUNT(*) as count
        FROM events_raw
        WHERE timestamp >= NOW() - INTERVAL '10 minutes'
    """)
    
    recent_count = recent_events[0]['count'] if recent_events else 0
    logger.info(f"Events in last 10 minutes: {recent_count}")
    
    if recent_count == 0:
        logger.warning("⚠ No recent events. Event generator may be stopped.")
        return False
    
    # Check event type distribution
    event_types = client.execute_query("""
        SELECT event_type, COUNT(*) as count
        FROM events_raw
        GROUP BY event_type
        ORDER BY count DESC
    """)
    
    logger.info("\nEvent type distribution:")
    for row in event_types:
        logger.info(f"  {row['event_type']}: {row['count']}")
    
    logger.info("✓ Event data looks good")
    return True


def check_aggregations():
    """Check if aggregations are being computed"""
    logger.info("\nChecking aggregations...")
    
    client = get_postgres_client()
    
    # Check 1-minute metrics
    recent_metrics = client.execute_query("""
        SELECT COUNT(*) as count
        FROM metrics_1min
        WHERE window_start >= NOW() - INTERVAL '1 hour'
    """)
    
    metrics_count = recent_metrics[0]['count'] if recent_metrics else 0
    logger.info(f"1-min metrics in last hour: {metrics_count}")
    
    if metrics_count == 0:
        logger.warning("⚠ No recent metrics. Are Spark jobs running?")
        return False
    
    # Check session summaries
    session_count = client.get_table_row_count('session_summary')
    logger.info(f"Total sessions tracked: {session_count}")
    
    logger.info("✓ Aggregations are being computed")
    return True


def check_minio_storage():
    """Check MinIO object storage"""
    logger.info("\nChecking MinIO storage...")
    
    try:
        client = get_minio_client()
        
        # Check bucket exists
        bucket_name = 'raw-events'
        if not client.bucket_exists(bucket_name):
            logger.error(f"✗ Bucket '{bucket_name}' does not exist")
            return False
        
        # List objects
        objects = client.list_objects(bucket_name, prefix='events/')
        object_count = len(objects)
        
        logger.info(f"Objects in {bucket_name}: {object_count}")
        
        if object_count > 0:
            # Show sample objects
            logger.info("\nSample objects:")
            for obj in objects[:5]:
                logger.info(f"  {obj}")
        
        # Get bucket size
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
    
    # Get recent DQ checks
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
        logger.warning("⚠ No data quality checks found. Is Airflow running?")
        return False
    
    logger.info("\nRecent DQ checks:")
    for check in dq_checks:
        status = "✓ PASS" if check['passed'] else "✗ FAIL"
        logger.info(f"  {check['check_name']} ({check['check_type']}): {status}")
    
    # Count failures
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
    
    # Summary
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

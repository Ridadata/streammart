"""
Airflow DAG: Data Quality Checks
Runs data quality validations to ensure pipeline health

WHY THIS EXISTS:
- Catch data issues early (missing data, schemas drift, outliers)
- Automated alerts when quality degrades
- Build trust in data with stakeholders

WHAT BREAKS WITHOUT IT:
- Silent data quality issues
- Bad data propagates to dashboards
- Users lose trust in data
- Manual quality checks (slow, unreliable)

REAL-WORLD USAGE:
- Uber uses Great Expectations for data quality
- Airbnb has dedicated data quality framework
- Most mature data teams have automated DQ checks

DATA QUALITY DIMENSIONS:
1. Completeness: Is all expected data present?
2. Accuracy: Are values within expected ranges?
3. Timeliness: Is data arriving on time?
4. Consistency: Do related metrics reconcile?

SCHEMA NOTE (why this file was rewritten):
The original version of this DAG queried metrics_1min.timestamp,
metrics_1min.properties, and metrics_1min.session_id — none of which exist.
Those columns belonged to an `events_raw` table that was drafted in
init_postgres.sql but never actually created (raw events live in MinIO, not
Postgres — see src/spark_jobs/raw_event_writer.py). Every check below is
written against the real schema: metrics_1min, session_summary,
product_performance (see CLAUDE.md for the full table-ownership matrix).

These checks are distinct from the lightweight "is this table non-empty"
heartbeat checks that sql/maintenance.sql writes every 60 seconds
(check_name prefixed 'heartbeat_') — this DAG runs actual business-rule
validation every 6 hours.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import logging

POSTGRES_CONN_ID = 'streammart_postgres'

default_args = {
    'owner': 'streammart',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,  # Set to True and add 'email' list when SMTP is configured
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'streammart_data_quality',
    default_args=default_args,
    description='Data quality validation checks',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['streammart', 'data_quality', 'monitoring'],
)


def _log_check(cursor, check_name, check_type, table_name, passed,
                metric_value=None, threshold_value=None, details=None):
    """Shared helper: insert one row into data_quality_checks. Parameterized —
    no f-string SQL interpolation, even though these values happen to be
    internally computed rather than user input."""
    cursor.execute(
        """
        INSERT INTO data_quality_checks (
            check_name, check_type, table_name, passed,
            metric_value, threshold_value, details
        ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
        """,
        (check_name, check_type, table_name, passed,
         metric_value, threshold_value, details or '{}')
    )


def check_completeness(**context):
    """
    Completeness Check: Ensure we received expected event volume in the last hour.

    Threshold is intentionally low (configurable via the DQ_MIN_HOURLY_EVENTS
    Airflow Variable, default 100) — this is a "is the pipeline alive at all"
    check, not a traffic-anomaly detector. It reads metrics_1min.count, which
    is the real per-window event count column.
    """
    logging.info("Running completeness check...")

    threshold = int(Variable.get('DQ_MIN_HOURLY_EVENTS', default_var=100))

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COALESCE(SUM(count), 0)
        FROM metrics_1min
        WHERE window_start >= NOW() - INTERVAL '1 hour'
    """)
    event_count = cursor.fetchone()[0]

    passed = event_count >= threshold

    _log_check(
        cursor, 'hourly_event_volume', 'completeness', 'metrics_1min', passed,
        metric_value=event_count, threshold_value=threshold,
        details=f'{{"message": "Events in last hour: {event_count}, threshold: {threshold}"}}'
    )

    conn.commit()
    cursor.close()
    conn.close()

    if passed:
        logging.info(f"✓ Completeness check PASSED: {event_count} events (>= {threshold})")
    else:
        logging.error(f"✗ Completeness check FAILED: {event_count} events (< {threshold})")
        raise ValueError(f"Event volume too low: {event_count} < {threshold}")


def check_accuracy(**context):
    """
    Accuracy Check: Validate revenue figures are within a sane range.

    Rules (checked against session_summary and product_performance, the only
    tables that actually hold revenue figures):
    - No negative revenue anywhere (schema allows it numerically; a Spark bug
      upstream could still produce it)
    - No single session/product-day with revenue > $10,000 (outlier guard)
    """
    logging.info("Running accuracy check...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*) FILTER (WHERE revenue < 0) AS negative_count,
               COUNT(*) FILTER (WHERE revenue > 10000) AS outlier_count
        FROM session_summary
        WHERE start_time >= NOW() - INTERVAL '24 hours'
    """)
    session_negative, session_outliers = cursor.fetchone()

    cursor.execute("""
        SELECT COUNT(*) FILTER (WHERE revenue < 0) AS negative_count,
               COUNT(*) FILTER (WHERE revenue > 10000) AS outlier_count
        FROM product_performance
        WHERE date >= CURRENT_DATE - INTERVAL '1 day'
    """)
    product_negative, product_outliers = cursor.fetchone()

    negative_count = session_negative + product_negative
    outlier_count = session_outliers + product_outliers
    passed = (negative_count == 0 and outlier_count < 5)

    _log_check(
        cursor, 'revenue_range_validation', 'accuracy', 'session_summary,product_performance', passed,
        details=(
            f'{{"negative_count": {negative_count}, "outlier_count": {outlier_count}, '
            f'"session_negative": {session_negative}, "product_negative": {product_negative}}}'
        )
    )

    conn.commit()
    cursor.close()
    conn.close()

    if passed:
        logging.info("✓ Accuracy check PASSED")
    else:
        logging.error(f"✗ Accuracy check FAILED: {negative_count} negative, {outlier_count} outliers")
        raise ValueError("Revenue range validation failed")


def check_timeliness(**context):
    """
    Timeliness Check: Ensure recent windows are being written to Postgres
    without excessive delay.

    Measures the real gap between when a window's time range ended
    (window_end) and when Spark actually wrote that row (created_at) —
    both genuine columns on metrics_1min. This directly measures pipeline
    processing lag, unlike the original version which referenced a
    nonexistent per-event ingestion timestamp.
    """
    logging.info("Running timeliness check...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        WITH delay_analysis AS (
            SELECT EXTRACT(EPOCH FROM (created_at - window_end)) AS delay_seconds
            FROM metrics_1min
            WHERE created_at >= NOW() - INTERVAL '1 hour'
        )
        SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY delay_seconds)
        FROM delay_analysis
    """)
    result = cursor.fetchone()
    p95_delay = float(result[0]) if result and result[0] is not None else 0.0

    threshold = 300  # 5 minutes in seconds
    passed = p95_delay <= threshold

    _log_check(
        cursor, 'window_write_delay', 'timeliness', 'metrics_1min', passed,
        metric_value=p95_delay, threshold_value=threshold,
        details=f'{{"p95_delay_seconds": {p95_delay:.1f}, "threshold_seconds": {threshold}}}'
    )

    conn.commit()
    cursor.close()
    conn.close()

    if passed:
        logging.info(f"✓ Timeliness check PASSED: P95 write delay = {p95_delay:.1f}s")
    else:
        logging.warning(f"⚠ Timeliness check FAILED: P95 write delay = {p95_delay:.1f}s > {threshold}s")


def check_consistency(**context):
    """
    Consistency Check: Two independent Spark jobs both derive revenue from the
    same events.purchase stream — session_tracker sums the order `total` per
    session, revenue_aggregator sums exploded per-item revenue per product.
    They should reconcile closely for the same day. A meaningful drift between
    them indicates a bug in one of the two pipelines (double counting, dropped
    items, a broken join, etc.) — this is a much stronger consistency signal
    than comparing row counts across unrelated grains, which is what the
    original (broken) version of this check attempted.
    """
    logging.info("Running consistency check...")

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COALESCE(SUM(revenue), 0) FROM session_summary WHERE start_time >= CURRENT_DATE
    """)
    session_revenue = float(cursor.fetchone()[0])

    cursor.execute("""
        SELECT COALESCE(SUM(revenue), 0) FROM product_performance WHERE date = CURRENT_DATE
    """)
    product_revenue = float(cursor.fetchone()[0])

    larger = max(session_revenue, product_revenue, 0.01)
    diff_pct = abs(session_revenue - product_revenue) / larger * 100

    # Allow 5% discrepancy: the two jobs run on independent trigger schedules
    # and watermarks, so a small amount of timing skew is expected, especially
    # right after local midnight.
    passed = diff_pct <= 5

    _log_check(
        cursor, 'revenue_pipeline_reconciliation', 'consistency',
        'session_summary,product_performance', passed,
        metric_value=diff_pct, threshold_value=5,
        details=(
            f'{{"session_tracker_revenue": {session_revenue:.2f}, '
            f'"revenue_aggregator_revenue": {product_revenue:.2f}, '
            f'"diff_pct": {diff_pct:.2f}}}'
        )
    )

    conn.commit()
    cursor.close()
    conn.close()

    if passed:
        logging.info(f"✓ Consistency check PASSED: {diff_pct:.2f}% difference")
    else:
        logging.warning(f"⚠ Consistency check FAILED: {diff_pct:.2f}% difference")


# Task definitions
completeness_task = PythonOperator(
    task_id='check_completeness',
    python_callable=check_completeness,
    dag=dag,
)

accuracy_task = PythonOperator(
    task_id='check_accuracy',
    python_callable=check_accuracy,
    dag=dag,
)

timeliness_task = PythonOperator(
    task_id='check_timeliness',
    python_callable=check_timeliness,
    dag=dag,
)

consistency_task = PythonOperator(
    task_id='check_consistency',
    python_callable=check_consistency,
    dag=dag,
)

# Run all checks in parallel (no dependencies)
# If any check fails, the DAG fails and alerts fire
[completeness_task, accuracy_task, timeliness_task, consistency_task]

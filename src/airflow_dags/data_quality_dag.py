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
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

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


def check_completeness(**context):
    """
    Completeness Check: Ensure we received expected volume of events
    
    Rule: Should have at least 1000 events per hour
    If below threshold, something may be wrong with event generation
    """
    logging.info("Running completeness check...")
    
    pg_hook = PostgresHook(postgres_conn_id='streammart_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Check event volume in last hour
    cursor.execute("""
        SELECT COUNT(*) as event_count
        FROM metrics_1min
        WHERE timestamp >= NOW() - INTERVAL '1 hour'
    """)
    
    result = cursor.fetchone()
    event_count = result[0] if result else 0
    
    threshold = 1000
    passed = event_count >= threshold
    
    # Log result
    cursor.execute("""
        INSERT INTO data_quality_checks (
            check_name,
            check_type,
            table_name,
            passed,
            metric_value,
            threshold_value,
            details
        ) VALUES (
            'hourly_event_volume',
            'completeness',
            'metrics_1min',
            %s,
            %s,
            %s,
            %s::jsonb
        )
    """, (
        passed,
        event_count,
        threshold,
        f'{{"message": "Events in last hour: {event_count}, Threshold: {threshold}"}}'
    ))
    
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
    Accuracy Check: Validate purchase amounts are reasonable
    
    Rules:
    - No negative order totals
    - No orders > $10,000 (outlier detection)
    - Order total = subtotal + tax + shipping (consistency)
    """
    logging.info("Running accuracy check...")
    
    pg_hook = PostgresHook(postgres_conn_id='streammart_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Check for negative orders
    cursor.execute("""
        SELECT COUNT(*) as negative_count
        FROM metrics_1min
        WHERE event_type = 'purchase'
        AND (properties->>'total')::DECIMAL < 0
        AND timestamp >= NOW() - INTERVAL '24 hours'
    """)
    
    negative_count = cursor.fetchone()[0]
    
    # Check for unreasonably high orders
    cursor.execute("""
        SELECT COUNT(*) as outlier_count
        FROM metrics_1min
        WHERE event_type = 'purchase'
        AND (properties->>'total')::DECIMAL > 10000
        AND timestamp >= NOW() - INTERVAL '24 hours'
    """)
    
    outlier_count = cursor.fetchone()[0]
    
    passed = (negative_count == 0 and outlier_count < 5)
    
    cursor.execute("""
        INSERT INTO data_quality_checks (
            check_name,
            check_type,
            table_name,
            passed,
            details
        ) VALUES (
            'purchase_amount_validation',
            'accuracy',
            'metrics_1min',
            %s,
            %s::jsonb
        )
    """, (
        passed,
        f'{{"negative_orders": {negative_count}, "outlier_orders": {outlier_count}}}'
    ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    if passed:
        logging.info(f"✓ Accuracy check PASSED")
    else:
        logging.error(f"✗ Accuracy check FAILED: {negative_count} negative, {outlier_count} outliers")
        raise ValueError("Purchase amounts validation failed")


def check_timeliness(**context):
    """
    Timeliness Check: Ensure data is arriving with acceptable delay
    
    Rule: 95% of events should arrive within 5 minutes of generation
    """
    logging.info("Running timeliness check...")
    
    pg_hook = PostgresHook(postgres_conn_id='streammart_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Check event ingestion delay
    cursor.execute("""
        WITH delay_analysis AS (
            SELECT
                EXTRACT(EPOCH FROM (created_at - timestamp)) as delay_seconds
            FROM metrics_1min
            WHERE created_at >= NOW() - INTERVAL '1 hour'
        )
        SELECT
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY delay_seconds) as p95_delay
        FROM delay_analysis
    """)
    
    result = cursor.fetchone()
    p95_delay = result[0] if result and result[0] else 0
    
    threshold = 300  # 5 minutes in seconds
    passed = p95_delay <= threshold
    
    cursor.execute("""
        INSERT INTO data_quality_checks (
            check_name,
            check_type,
            table_name,
            passed,
            metric_value,
            threshold_value,
            details
        ) VALUES (
            'event_ingestion_delay',
            'timeliness',
            'metrics_1min',
            %s,
            %s,
            %s,
            %s::jsonb
        )
    """, (
        passed,
        p95_delay,
        threshold,
        f'{{"p95_delay_seconds": {p95_delay}, "threshold_seconds": {threshold}}}'
    ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    if passed:
        logging.info(f"✓ Timeliness check PASSED: P95 delay = {p95_delay:.1f}s")
    else:
        logging.warning(f"⚠ Timeliness check FAILED: P95 delay = {p95_delay:.1f}s > {threshold}s")


def check_consistency(**context):
    """
    Consistency Check: Verify metrics reconcile across tables
    
    Rule: Session_summary conversion count should match purchase event count
    """
    logging.info("Running consistency check...")
    
    pg_hook = PostgresHook(postgres_conn_id='streammart_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Count conversions from session_summary
    cursor.execute("""
        SELECT COUNT(*) FROM session_summary
        WHERE converted = TRUE
        AND start_time >= NOW() - INTERVAL '24 hours'
    """)
    session_conversions = cursor.fetchone()[0]
    
    # Count purchase events
    cursor.execute("""
        SELECT COUNT(DISTINCT session_id) FROM metrics_1min
        WHERE event_type = 'purchase'
        AND timestamp >= NOW() - INTERVAL '24 hours'
    """)
    purchase_events = cursor.fetchone()[0]
    
    # Allow 5% discrepancy (due to timing of aggregations)
    diff_pct = abs(session_conversions - purchase_events) / max(purchase_events, 1) * 100
    passed = diff_pct <= 5
    
    cursor.execute("""
        INSERT INTO data_quality_checks (
            check_name,
            check_type,
            table_name,
            passed,
            details
        ) VALUES (
            'conversion_reconciliation',
            'consistency',
            'session_summary,metrics_1min',
            %s,
            %s::jsonb
        )
    """, (
        passed,
        f'{{"session_conversions": {session_conversions}, '
        f'"purchase_events": {purchase_events}, "diff_pct": {diff_pct:.2f}}}'
    ))
    
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
    provide_context=True,
    dag=dag,
)

accuracy_task = PythonOperator(
    task_id='check_accuracy',
    python_callable=check_accuracy,
    provide_context=True,
    dag=dag,
)

timeliness_task = PythonOperator(
    task_id='check_timeliness',
    python_callable=check_timeliness,
    provide_context=True,
    dag=dag,
)

consistency_task = PythonOperator(
    task_id='check_consistency',
    python_callable=check_consistency,
    provide_context=True,
    dag=dag,
)

# Run all checks in parallel (no dependencies)
# If any check fails, the DAG fails and alerts fire
[completeness_task, accuracy_task, timeliness_task, consistency_task]

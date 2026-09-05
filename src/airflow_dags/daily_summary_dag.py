"""
Airflow DAG: Daily Summary
Computes daily business metrics and populates the daily_revenue table

WHY THIS EXISTS:
- Batch processing layer complements real-time streaming
- Daily rollups reduce query load on dashboards
- Historical trend analysis
- Data quality reconciliation (streaming vs batch results)

WHAT BREAKS WITHOUT IT:
- Can't show daily/weekly/monthly trends
- Dashboard queries become slow (must scan all raw data)
- No single source of truth for daily metrics

REAL-WORLD USAGE:
- Netflix computes daily viewing metrics for content teams
- Airbnb calculates daily booking metrics for hosts
- Most companies have daily summary tables for executive dashboards

IDEMPOTENCY:
- This DAG can be run multiple times for the same date
- Uses UPSERT (INSERT ... ON CONFLICT DO UPDATE)
- Backfills are safe

OWNERSHIP:
This DAG is the sole writer of daily_revenue (see ENGINEERING.md table-ownership
matrix). sql/maintenance.sql previously also wrote to this table on every
60-second tick with no date filter — that dual-writer setup, plus the missing
filter, is why daily_revenue used to accumulate all-time totals mislabeled as
"today". Both bugs are fixed by this DAG being the only writer, filtered to
its own execution date.

SCHEMA NOTE (why this file was rewritten):
The original version queried metrics_1min.timestamp / .properties /
.session_id, none of which exist — that was written against a
never-created raw-events table draft. The real source for per-session revenue
and conversion data is session_summary (written by session_tracker.py),
which is what this version uses.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
import logging

POSTGRES_CONN_ID = 'streammart_postgres'

# Default arguments
default_args = {
    'owner': 'streammart',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'streammart_daily_summary',
    default_args=default_args,
    description='Compute daily business metrics',
    schedule_interval='0 1 * * *',  # Run at 1 AM daily
    catchup=False,  # Don't backfill automatically
    max_active_runs=1,
    tags=['streammart', 'daily', 'metrics'],
)


def compute_daily_revenue(**context):
    """
    Compute daily revenue metrics from session_summary for this DAG run's date.

    This demonstrates the Lambda Architecture pattern:
    - Streaming layer (session_tracker.py): real-time, per-session approximation
    - Batch layer (this DAG): daily reconciliation, authoritative for reporting

    conversion_rate / cart_abandonment_rate are stored as fractions (0-1),
    matching daily_revenue's DECIMAL(5,4) columns — not percentages. Contrast
    with daily_summary.conversion_rate (DECIMAL(5,2)), which IS a percentage;
    that inconsistency predates this rewrite and is a known schema quirk
    (see ENGINEERING.md technical debt notes) rather than something safe to change
    here without touching every reader of both tables.
    """
    target_date = context['ds']  # YYYY-MM-DD format
    logging.info(f"Computing daily revenue for {target_date}")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        WITH day_sessions AS (
            SELECT *
            FROM session_summary
            WHERE DATE(start_time) = %(target_date)s
        )
        INSERT INTO daily_revenue (
            date, total_revenue, total_orders, unique_customers,
            avg_order_value, conversion_rate, cart_abandonment_rate,
            created_at, updated_at
        )
        SELECT
            %(target_date)s::date,
            COALESCE(SUM(revenue), 0.00) AS total_revenue,
            COUNT(*) FILTER (WHERE converted = TRUE) AS total_orders,
            COUNT(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL) AS unique_customers,
            COALESCE(
                ROUND(SUM(revenue) FILTER (WHERE converted = TRUE)
                      / NULLIF(COUNT(*) FILTER (WHERE converted = TRUE), 0), 2),
                0.00
            ) AS avg_order_value,
            COALESCE(
                ROUND((COUNT(*) FILTER (WHERE converted = TRUE)::NUMERIC
                       / NULLIF(COUNT(*), 0)), 4),
                0.0000
            ) AS conversion_rate,
            COALESCE(
                ROUND((COUNT(*) FILTER (WHERE converted = FALSE)::NUMERIC
                       / NULLIF(COUNT(*), 0)), 4),
                0.0000
            ) AS cart_abandonment_rate,
            NOW(),
            NOW()
        FROM day_sessions
        ON CONFLICT (date) DO UPDATE SET
            total_revenue         = EXCLUDED.total_revenue,
            total_orders          = EXCLUDED.total_orders,
            unique_customers      = EXCLUDED.unique_customers,
            avg_order_value       = EXCLUDED.avg_order_value,
            conversion_rate       = EXCLUDED.conversion_rate,
            cart_abandonment_rate = EXCLUDED.cart_abandonment_rate,
            updated_at            = NOW()
        """,
        {'target_date': target_date}
    )
    conn.commit()

    cursor.execute("SELECT * FROM daily_revenue WHERE date = %s", (target_date,))
    result = cursor.fetchone()

    if result:
        logging.info("✓ Daily metrics computed:")
        logging.info(f"  Revenue: ${result[1]:.2f}")
        logging.info(f"  Orders: {result[2]}")
        logging.info(f"  Customers: {result[3]}")
        logging.info(f"  AOV: ${result[4]:.2f}")
        logging.info(f"  Conversion Rate: {float(result[5]) * 100:.2f}%")
        logging.info(f"  Abandonment Rate: {float(result[6]) * 100:.2f}%")
    else:
        logging.warning(f"No data found for {target_date}")

    cursor.close()
    conn.close()


def refresh_materialized_views(**context):
    """Refresh all materialized views for dashboard queries"""
    logging.info("Refreshing materialized views...")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Call the refresh function we defined in init_postgres.sql
    cursor.execute("SELECT refresh_all_materialized_views();")
    conn.commit()

    logging.info("✓ Materialized views refreshed")

    cursor.close()
    conn.close()


def log_pipeline_status(**context):
    """Log pipeline execution status"""
    target_date = context['ds']

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO pipeline_monitoring (
            job_name, job_type, start_time, end_time, status, metadata
        ) VALUES (
            'daily_summary_dag', 'airflow_dag',
            NOW() - INTERVAL '1 minute', NOW(), 'success', %s::jsonb
        )
        """,
        (json.dumps({'execution_date': target_date}),)
    )

    conn.commit()
    cursor.close()
    conn.close()

    logging.info(f"✓ Pipeline status logged for {target_date}")


# Task definitions
compute_revenue_task = PythonOperator(
    task_id='compute_daily_revenue',
    python_callable=compute_daily_revenue,
    dag=dag,
)

refresh_views_task = PythonOperator(
    task_id='refresh_materialized_views',
    python_callable=refresh_materialized_views,
    dag=dag,
)

log_status_task = PythonOperator(
    task_id='log_pipeline_status',
    python_callable=log_pipeline_status,
    dag=dag,
)

# Task dependencies
# Linear flow: compute → refresh → log
compute_revenue_task >> refresh_views_task >> log_status_task

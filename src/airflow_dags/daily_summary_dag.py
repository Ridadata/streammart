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
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

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
    Compute daily revenue metrics from metrics_1min
    
    This demonstrates the Lambda Architecture pattern:
    - Streaming layer: Real-time metrics (not 100% accurate)
    - Batch layer: Daily reconciliation (authoritative)
    """
    execution_date = context['ds']  # YYYY-MM-DD format
    
    logging.info(f"Computing daily revenue for {execution_date}")
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='streammart_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Compute metrics from raw events
    query = f"""
    WITH purchase_events AS (
        SELECT
            DATE(timestamp) as date,
            properties->>'order_id' as order_id,
            properties->>'user_id' as user_id,
            (properties->>'total')::DECIMAL as order_total
        FROM metrics_1min
        WHERE event_type = 'purchase'
        AND DATE(timestamp) = '{execution_date}'
    ),
    daily_metrics AS (
        SELECT
            date,
            SUM(order_total) as total_revenue,
            COUNT(DISTINCT order_id) as total_orders,
            COUNT(DISTINCT user_id) as unique_customers,
            ROUND(AVG(order_total), 2) as avg_order_value
        FROM purchase_events
        GROUP BY date
    ),
    conversion_metrics AS (
        SELECT
            COUNT(DISTINCT CASE WHEN event_type = 'pageview' THEN session_id END) as sessions,
            COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN session_id END) as conversions,
            COUNT(DISTINCT CASE WHEN event_type = 'abandonment' THEN session_id END) as abandonments
        FROM metrics_1min
        WHERE DATE(timestamp) = '{execution_date}'
    )
    INSERT INTO daily_revenue (
        date,
        total_revenue,
        total_orders,
        unique_customers,
        avg_order_value,
        conversion_rate,
        cart_abandonment_rate,
        created_at,
        updated_at
    )
    SELECT
        dm.date,
        COALESCE(dm.total_revenue, 0) as total_revenue,
        COALESCE(dm.total_orders, 0) as total_orders,
        COALESCE(dm.unique_customers, 0) as unique_customers,
        COALESCE(dm.avg_order_value, 0) as avg_order_value,
        CASE
            WHEN cm.sessions > 0 THEN
                ROUND((cm.conversions::DECIMAL / cm.sessions) * 100, 4)
            ELSE 0
        END as conversion_rate,
        CASE
            WHEN (cm.conversions + cm.abandonments) > 0 THEN
                ROUND((cm.abandonments::DECIMAL / (cm.conversions + cm.abandonments)) * 100, 4)
            ELSE 0
        END as cart_abandonment_rate,
        NOW() as created_at,
        NOW() as updated_at
    FROM daily_metrics dm
    CROSS JOIN conversion_metrics cm
    ON CONFLICT (date)
    DO UPDATE SET
        total_revenue = EXCLUDED.total_revenue,
        total_orders = EXCLUDED.total_orders,
        unique_customers = EXCLUDED.unique_customers,
        avg_order_value = EXCLUDED.avg_order_value,
        conversion_rate = EXCLUDED.conversion_rate,
        cart_abandonment_rate = EXCLUDED.cart_abandonment_rate,
        updated_at = NOW();
    """
    
    cursor.execute(query)
    conn.commit()
    
    # Fetch result for logging
    cursor.execute(f"SELECT * FROM daily_revenue WHERE date = '{execution_date}'")
    result = cursor.fetchone()
    
    if result:
        logging.info(f"✓ Daily metrics computed:")
        logging.info(f"  Revenue: ${result[1]:.2f}")
        logging.info(f"  Orders: {result[2]}")
        logging.info(f"  Customers: {result[3]}")
        logging.info(f"  AOV: ${result[4]:.2f}")
        logging.info(f"  Conversion Rate: {result[5]:.2f}%")
        logging.info(f"  Abandonment Rate: {result[6]:.2f}%")
    else:
        logging.warning(f"No data found for {execution_date}")
    
    cursor.close()
    conn.close()


def refresh_materialized_views(**context):
    """Refresh all materialized views for dashboard queries"""
    logging.info("Refreshing materialized views...")
    
    pg_hook = PostgresHook(postgres_conn_id='streammart_postgres')
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
    execution_date = context['ds']
    
    pg_hook = PostgresHook(postgres_conn_id='streammart_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO pipeline_monitoring (
            job_name,
            job_type,
            start_time,
            end_time,
            status,
            metadata
        ) VALUES (
            'daily_summary_dag',
            'airflow_dag',
            NOW() - INTERVAL '1 minute',
            NOW(),
            'success',
            %s::jsonb
        )
    """, (f'{{"execution_date": "{execution_date}"}}',))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logging.info(f"✓ Pipeline status logged for {execution_date}")


# Task definitions
compute_revenue_task = PythonOperator(
    task_id='compute_daily_revenue',
    python_callable=compute_daily_revenue,
    provide_context=True,
    dag=dag,
)

refresh_views_task = PythonOperator(
    task_id='refresh_materialized_views',
    python_callable=refresh_materialized_views,
    provide_context=True,
    dag=dag,
)

log_status_task = PythonOperator(
    task_id='log_pipeline_status',
    python_callable=log_pipeline_status,
    provide_context=True,
    dag=dag,
)

# Task dependencies
# Linear flow: compute → refresh → log
compute_revenue_task >> refresh_views_task >> log_status_task

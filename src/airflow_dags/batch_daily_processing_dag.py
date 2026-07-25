"""
Airflow DAG: Batch Daily Summary Pipeline
Computes daily business metrics and aggregations from streaming data

PURPOSE:
- Complements real-time streaming with offline batch processing
- Computes daily rollups for executive dashboards
- Enables historical trend analysis and forecasting
- Provides reconciliation checkpoints (streaming vs batch)

RUN FREQUENCY:
- Runs once daily at 2 AM (after streaming overnight)
- Allows backfilling for historical dates

IDEMPOTENCY:
- Uses UPSERT (INSERT ... ON CONFLICT DO UPDATE)
- Safe to rerun for same date
- Depends on completed streaming data for the date

KEY METRICS:
- Total events per event type
- Revenue and conversion metrics
- Session quality scores
- Top products and categories
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
import logging

logger = logging.getLogger(__name__)

# ==============================================================================
# TASK FUNCTIONS
# ==============================================================================

def validate_data_availability(**context):
    """Check if sufficient streaming data exists for the target date"""
    target_date = context['ds']  # YYYY-MM-DD
    
    try:
        hook = PostgresHook(postgres_conn_id='streammart_postgres')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Count metrics for target date
        cursor.execute(f"""
            SELECT COUNT(*) as metric_count
            FROM metrics_1min
            WHERE DATE(window_end) = '{target_date}'
        """)
        result = cursor.fetchone()
        metric_count = result[0]
        
        logger.info(f"Found {metric_count} metrics for {target_date}")
        
        if metric_count < 100:  # Arbitrary threshold for "sufficient data"
            logger.warning(
                f"⚠ Only {metric_count} metrics for {target_date}. "
                f"Streaming may have been offline. Proceeding anyway..."
            )
        
        cursor.close()
        conn.close()
        return 'compute_daily_summary'  # Proceed to next task
    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}")
        return 'skip_processing'  # Skip if data unavailable


def compute_daily_summary(**context):
    """Compute and insert daily aggregated metrics"""
    target_date = context['ds']
    
    summary_sql = f"""
    WITH event_counts AS (
        SELECT
            DATE(window_start) AS date,
            SUM(count) AS total_events,
            SUM(CASE WHEN event_type = 'pageview'       THEN count ELSE 0 END) AS pageview_events,
            SUM(CASE WHEN event_type = 'product_click'  THEN count ELSE 0 END) AS product_click_events,
            SUM(CASE WHEN event_type = 'add_to_cart'    THEN count ELSE 0 END) AS add_to_cart_events,
            SUM(CASE WHEN event_type = 'purchase'       THEN count ELSE 0 END) AS purchase_events,
            SUM(CASE WHEN event_type = 'abandonment'    THEN count ELSE 0 END) AS abandonment_events
        FROM metrics_1min
        WHERE DATE(window_start) = '{target_date}'
        GROUP BY DATE(window_start)
    ),
    session_stats AS (
        SELECT
            DATE(start_time) AS date,
            COUNT(*) AS unique_sessions,
            COALESCE(SUM(revenue), 0) AS total_revenue,
            ROUND(100.0 * COUNT(CASE WHEN converted THEN 1 END) /
                  NULLIF(COUNT(*), 0), 2) AS conversion_rate,
            ROUND(AVG(duration_seconds), 2) AS avg_session_duration_sec
        FROM session_summary
        WHERE DATE(start_time) = '{target_date}'
        GROUP BY DATE(start_time)
    )
    INSERT INTO daily_summary (
        date, total_events, pageview_events, product_click_events,
        add_to_cart_events, purchase_events, abandonment_events,
        unique_sessions, total_revenue, conversion_rate,
        avg_session_duration_sec, computed_at
    )
    SELECT
        COALESCE(ec.date, ss.date),
        COALESCE(ec.total_events, 0),
        COALESCE(ec.pageview_events, 0),
        COALESCE(ec.product_click_events, 0),
        COALESCE(ec.add_to_cart_events, 0),
        COALESCE(ec.purchase_events, 0),
        COALESCE(ec.abandonment_events, 0),
        COALESCE(ss.unique_sessions, 0),
        COALESCE(ss.total_revenue, 0.00),
        COALESCE(ss.conversion_rate, 0.00),
        COALESCE(ss.avg_session_duration_sec, 0.00),
        NOW()
    FROM event_counts ec
    FULL OUTER JOIN session_stats ss ON ec.date = ss.date
    ON CONFLICT (date) DO UPDATE SET
        total_events             = EXCLUDED.total_events,
        pageview_events          = EXCLUDED.pageview_events,
        product_click_events     = EXCLUDED.product_click_events,
        add_to_cart_events       = EXCLUDED.add_to_cart_events,
        purchase_events          = EXCLUDED.purchase_events,
        abandonment_events       = EXCLUDED.abandonment_events,
        unique_sessions          = EXCLUDED.unique_sessions,
        total_revenue            = EXCLUDED.total_revenue,
        conversion_rate          = EXCLUDED.conversion_rate,
        avg_session_duration_sec = EXCLUDED.avg_session_duration_sec,
        computed_at              = EXCLUDED.computed_at
    """
    
    try:
        hook = PostgresHook(postgres_conn_id='streammart_postgres')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(summary_sql)
        conn.commit()
        rows = cursor.rowcount
        logger.info(f"✓ Computed daily summary: {rows} rows affected for {target_date}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        raise AirflowException(f"Daily summary computation failed: {str(e)}")


def compute_top_products(**context):
    """Compute top performing products by revenue"""
    target_date = context['ds']
    
    top_products_sql = f"""
    INSERT INTO product_daily_performance (
        date, product_id, total_clicks, total_purchases, total_revenue, conversion_rate
    )
    SELECT
        date,
        product_id,
        click_count                                                         AS total_clicks,
        purchase_count                                                      AS total_purchases,
        revenue                                                             AS total_revenue,
        ROUND(
            100.0 * purchase_count /
            NULLIF(click_count + add_to_cart_count, 0),
            2
        )                                                                   AS conversion_rate
    FROM product_performance
    WHERE date = '{target_date}'::date
    ON CONFLICT (date, product_id) DO UPDATE SET
        total_clicks    = EXCLUDED.total_clicks,
        total_purchases = EXCLUDED.total_purchases,
        total_revenue   = EXCLUDED.total_revenue,
        conversion_rate = EXCLUDED.conversion_rate
    """
    
    try:
        hook = PostgresHook(postgres_conn_id='streammart_postgres')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(top_products_sql)
        conn.commit()
        rows = cursor.rowcount
        logger.info(f"✓ Computed product performance: {rows} rows affected for {target_date}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.warning(f"Product performance computation skipped (table may not exist): {str(e)}")
        return True


def validate_summary_results(**context):
    """Quality check: validate computed summary makes sense"""
    target_date = context['ds']
    
    try:
        hook = PostgresHook(postgres_conn_id='streammart_postgres')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT 
                total_events,
                unique_sessions,
                total_revenue,
                conversion_rate
            FROM daily_summary
            WHERE date = '{target_date}'
        """)
        result = cursor.fetchone()
        
        if not result:
            raise AirflowException(f"No summary record found for {target_date}")
        
        total_events, unique_sessions, total_revenue, conversion_rate = result
        
        logger.info(
            f"✓ Daily summary validated for {target_date}:\n"
            f"  - Total events: {total_events}\n"
            f"  - Unique sessions: {unique_sessions}\n"
            f"  - Total revenue: ${total_revenue}\n"
            f"  - Conversion rate: {conversion_rate}%"
        )
        
        # Sanity checks
        if conversion_rate and (conversion_rate < 0 or conversion_rate > 100):
            raise AirflowException(f"Invalid conversion rate: {conversion_rate}%")
        
        if unique_sessions and total_events and (total_events < unique_sessions):
            logger.warning(
                f"⚠ Unusual ratio: {total_events} events for {unique_sessions} sessions"
            )
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        raise AirflowException(f"Summary validation failed: {str(e)}")


# ==============================================================================
# DAG DEFINITION
# ==============================================================================

default_args = {
    'owner': 'streammart',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    'streammart_daily_batch_processing',
    default_args=default_args,
    description='Batch daily aggregations and business metrics computation',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily (after overnight streaming)
    # catchup=False: with start_date fixed at 2026-03-01, catchup=True would
    # trigger one DAG run per day between start_date and whenever this DAG is
    # first unpaused — potentially well over a hundred backfill runs firing at
    # once against a small local Postgres instance. Every task here is
    # idempotent (UPSERT on a date-keyed primary key), so an intentional
    # historical backfill is still fully supported — just trigger it
    # explicitly and bounded: `airflow dags backfill streammart_daily_batch_processing
    # -s 2026-03-01 -e 2026-03-15`. That's the standard, controlled way to
    # backfill in Airflow; relying on catchup=True to do it implicitly on
    # unpause is what caused the uncontrolled-backfill risk this comment
    # replaces.
    catchup=False,
    tags=['batch', 'aggregation', 'business-metrics'],
)

# Task definitions
start = DummyOperator(task_id='start', dag=dag)

validate_data = BranchPythonOperator(
    task_id='validate_input_data',
    python_callable=validate_data_availability,
    dag=dag,
)

compute_summary = PythonOperator(
    task_id='compute_daily_summary',
    python_callable=compute_daily_summary,
    dag=dag,
)

compute_products = PythonOperator(
    task_id='compute_top_products',
    python_callable=compute_top_products,
    dag=dag,
)

validate_results = PythonOperator(
    task_id='validate_summary_results',
    python_callable=validate_summary_results,
    dag=dag,
)

skip_processing = DummyOperator(task_id='skip_processing', dag=dag)
end = DummyOperator(task_id='end', trigger_rule='one_success', dag=dag)

# Task dependencies
start >> validate_data >> [compute_summary, skip_processing]
compute_summary >> compute_products >> validate_results >> end
skip_processing >> end

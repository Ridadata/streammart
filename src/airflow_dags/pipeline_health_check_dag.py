"""
Airflow DAG: StreamMart Pipeline Monitoring and Health Checks
Monitors streaming jobs, data quality, and infrastructure health

PURPOSE:
- Validates streaming job status and data freshness
- Checks PostgreSQL data quality and completeness
- Monitors storage (MinIO) partition health
- Alerts on pipeline anomalies

RUN FREQUENCY:
- Runs hourly to detect issues quickly
- Fast fail-fast pattern (5-minute timeout per task)

IDEMPOTENCY:
- All checks are read-only, safe to run multiple times
- No state changes across reruns

FIXES IN THIS REWRITE (see ENGINEERING.md roadmap):
- Postgres connection id was set to a non-provisioned default in some
  tasks; now everything uses 'streammart_postgres', the one connection
  airflow-init actually creates.
- check_data_quality_metrics queried a non-existent converted-flag column, which
  doesn't exist — the real column is `converted`.
- check_session_data_volume's 10-minute staleness threshold guaranteed a
  false "stagnant" warning on every hourly run once session_tracker.py moved
  to append-mode output (sessions now land ~40-70 min after their last
  event by design — see session_tracker.py). Threshold raised to 90 minutes
  to match that latency profile instead of fighting it.
- MinIO credentials no longer fall back to hardcoded 'minioadmin' defaults;
  they're required env vars (see docker-compose.yml airflow-webserver/
  airflow-scheduler environment blocks) and boto3 is imported lazily inside
  the task so a missing dependency fails one task, not the whole DAG's parse.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
import logging

logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = 'streammart_postgres'

# ==============================================================================
# TASK FUNCTIONS
# ==============================================================================

def check_postgres_connectivity():
    """Verify PostgreSQL connection and database health"""
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        result = cursor.fetchone()
        logger.info(f"PostgreSQL version: {result[0]}")
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        raise AirflowException(f"PostgreSQL connection failed: {str(e)}")


def check_metrics_data_freshness():
    """Verify metrics_1min table has recent data (within last 5 minutes).

    window_aggregator.py writes on a 30-second trigger, so under normal
    operation the newest window should always be well under 5 minutes old
    regardless of when this hourly check happens to run.
    """
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT MAX(window_end) as latest_window
            FROM metrics_1min
        """)
        result = cursor.fetchone()

        if result[0] is None:
            raise AirflowException("No data in metrics_1min table")

        latest = result[0]
        age_minutes = (datetime.utcnow() - latest.replace(tzinfo=None)).total_seconds() / 60

        if age_minutes > 5:
            raise AirflowException(
                f"Metrics data is stale: {age_minutes:.1f} minutes old. "
                f"Latest window: {latest}. Check if WindowAggregator is running."
            )

        logger.info(f"✓ Metrics data is fresh: {age_minutes:.1f} minutes old")
        cursor.close()
        conn.close()
        return True
    except AirflowException:
        raise
    except Exception as e:
        raise AirflowException(f"Metrics freshness check failed: {str(e)}")


def check_session_data_volume():
    """Verify session_summary table grows (no data stagnation).

    Threshold is 90 minutes, not 10: session_tracker.py emits sessions in
    Structured Streaming "append" output mode (required for session_window
    aggregations — Spark does not support "update" mode for them). That means
    a session only appears in this table once the watermark confirms it's
    closed, roughly 40-70 minutes after its last event. A 10-minute threshold
    would false-positive on every single run of this check.
    """
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                COUNT(*) as total_rows,
                MAX(created_at) as latest_update
            FROM session_summary
        """)
        result = cursor.fetchone()
        total_rows, latest_update = result

        if total_rows == 0:
            raise AirflowException("No sessions in session_summary table")

        age_minutes = (datetime.utcnow() - latest_update.replace(tzinfo=None)).total_seconds() / 60
        staleness_threshold_minutes = 90

        if age_minutes > staleness_threshold_minutes:
            logger.warning(
                f"⚠ Session data may be stagnant: {age_minutes:.1f} minutes since update "
                f"(threshold: {staleness_threshold_minutes}m). Total rows: {total_rows}"
            )
        else:
            logger.info(
                f"✓ Session data is flowing: {total_rows} sessions, "
                f"latest update {age_minutes:.1f} minutes ago"
            )

        cursor.close()
        conn.close()
        return True
    except AirflowException:
        raise
    except Exception as e:
        raise AirflowException(f"Session data volume check failed: {str(e)}")


def check_event_distribution():
    """Verify all event types are being processed (no single-type bottleneck)"""
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                event_type,
                SUM(count) as total_events
            FROM metrics_1min
            WHERE window_end > NOW() - INTERVAL '1 hour'
            GROUP BY event_type
            ORDER BY total_events DESC
        """)
        results = cursor.fetchall()

        if not results:
            raise AirflowException("No metrics for last hour")

        event_types = [r[0] for r in results]
        expected_types = {'pageview', 'product_click', 'add_to_cart', 'purchase', 'abandonment'}
        found_types = set(event_types)

        if not expected_types.issubset(found_types):
            missing = expected_types - found_types
            raise AirflowException(f"Missing event types: {missing}")

        logger.info("✓ Event distribution:")
        for event_type, count in results:
            logger.info(f"  - {event_type}: {count} events")

        cursor.close()
        conn.close()
        return True
    except AirflowException:
        raise
    except Exception as e:
        raise AirflowException(f"Event distribution check failed: {str(e)}")


def check_minio_partitions():
    """Verify MinIO S3 partitions are being created (date-based folders exist)"""
    try:
        import boto3
        from botocore.exceptions import ClientError

        access_key = os.getenv('MINIO_ACCESS_KEY')
        secret_key = os.getenv('MINIO_SECRET_KEY')
        bucket = os.getenv('MINIO_BUCKET')
        endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')

        if not access_key or not secret_key or not bucket:
            raise AirflowException(
                "MINIO_ACCESS_KEY, MINIO_SECRET_KEY and MINIO_BUCKET must be set on the "
                "Airflow webserver/scheduler containers (see docker-compose.yml) — refusing "
                "to fall back to hardcoded default credentials."
            )

        s3_client = boto3.client(
            's3',
            endpoint_url=f'http://{endpoint}',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'
        )

        response = s3_client.list_objects_v2(Bucket=bucket, Prefix='events/', Delimiter='/')

        if 'CommonPrefixes' not in response:
            raise AirflowException(f"No partitions found in {bucket}/events/. Check if RawEventWriter is running.")

        years = [p['Prefix'] for p in response.get('CommonPrefixes', [])]
        logger.info(f"✓ MinIO partitions (year-level): {years}")

        if years:
            # Check latest year structure
            latest_year = sorted(years)[-1]
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=latest_year, Delimiter='/')
            months = [p['Prefix'].split('/')[-2] for p in response.get('CommonPrefixes', [])]
            logger.info(f"  Months in {latest_year.strip('/')}: {months}")

        return True
    except AirflowException:
        raise
    except ClientError as e:
        raise AirflowException(f"MinIO connection failed: {str(e)}")
    except Exception as e:
        raise AirflowException(f"MinIO partition check failed: {str(e)}")


def check_data_quality_metrics():
    """Validate key data quality metrics.

    Note the real column is `converted` (BOOLEAN) — the original version of
    this check queried a nonexistent converted-flag column and always failed.
    """
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                COUNT(*) FILTER (WHERE converted = TRUE) as converted,
                COUNT(*) FILTER (WHERE converted = FALSE) as not_converted,
                ROUND(100.0 * COUNT(*) FILTER (WHERE converted = TRUE) /
                      NULLIF(COUNT(*), 0), 2) as conversion_rate
            FROM session_summary
            WHERE created_at > NOW() - INTERVAL '24 hours'
        """)
        result = cursor.fetchone()
        converted, not_converted, conversion_rate = result

        if conversion_rate is None:
            logger.warning("⚠ No sessions in the last 24 hours — nothing to validate yet")
            cursor.close()
            conn.close()
            return True

        if conversion_rate < 0:
            raise AirflowException("Invalid conversion rate calculated")

        logger.info(
            f"✓ Conversion metrics (last 24h):\n"
            f"  - Converted: {converted}\n"
            f"  - Not converted: {not_converted}\n"
            f"  - Conversion rate: {conversion_rate}%"
        )

        cursor.close()
        conn.close()
        return True
    except AirflowException:
        raise
    except Exception as e:
        raise AirflowException(f"Data quality check failed: {str(e)}")


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
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=5),
}

dag = DAG(
    'streammart_pipeline_health_check',
    default_args=default_args,
    description='Monitor StreamMart pipeline health and data quality',
    schedule_interval='0 * * * *',  # Run every hour
    catchup=False,
    tags=['monitoring', 'health-check'],
)

# Task definitions
start = DummyOperator(task_id='start', dag=dag)

check_postgres = PythonOperator(
    task_id='check_postgres_connectivity',
    python_callable=check_postgres_connectivity,
    dag=dag,
)

check_metrics = PythonOperator(
    task_id='check_metrics_freshness',
    python_callable=check_metrics_data_freshness,
    dag=dag,
)

check_sessions = PythonOperator(
    task_id='check_session_data_volume',
    python_callable=check_session_data_volume,
    dag=dag,
)

check_events = PythonOperator(
    task_id='check_event_distribution',
    python_callable=check_event_distribution,
    dag=dag,
)

check_storage = PythonOperator(
    task_id='check_minio_partitions',
    python_callable=check_minio_partitions,
    dag=dag,
)

check_quality = PythonOperator(
    task_id='check_data_quality_metrics',
    python_callable=check_data_quality_metrics,
    dag=dag,
)

end = DummyOperator(task_id='end', trigger_rule='all_done', dag=dag)

# Task dependencies
start >> check_postgres >> [check_metrics, check_sessions, check_events, check_storage, check_quality] >> end

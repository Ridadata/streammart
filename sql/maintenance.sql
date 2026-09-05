-- ============================================================================
-- StreamMart Postgres Maintenance Job
-- Runs every 60 seconds inside the `postgres-maintenance` container.
--
-- NOTE: this file used to be called rollups.sql and also derived metrics_5min
-- from metrics_1min. That's gone — metrics_5min is now computed directly by
-- window_aggregator.py (Spark), independently of metrics_1min, because you
-- cannot correctly derive a 5-minute distinct-session count by summing five
-- 1-minute approx-distinct counts (see window_aggregator.py module docstring
-- for the full explanation). This script's remaining job is infra heartbeats
-- and housekeeping — not business-metric computation. Table ownership for
-- every table in this schema is documented in ENGINEERING.md; do not add a second
-- writer to any table without updating that matrix first.
-- ============================================================================

-- 1) Data quality heartbeat (hourly overwrite)
--
-- These are lightweight, always-on liveness checks ("is metrics_1min getting
-- rows at all?"), distinct from the business-rule quality checks the Airflow
-- `streammart_data_quality` DAG runs every 6 hours (negative-order detection,
-- outlier detection, conversion reconciliation, etc.). Different check_name
-- values, so the two never collide in data_quality_checks.
DELETE FROM data_quality_checks
WHERE check_name IN ('heartbeat_metrics_1min_non_empty', 'heartbeat_session_summary_non_empty', 'heartbeat_freshness_metrics_1min')
  AND check_timestamp >= date_trunc('hour', NOW());

INSERT INTO data_quality_checks (check_name, check_type, table_name, passed, metric_value, threshold_value, details)
SELECT
    'heartbeat_metrics_1min_non_empty',
    'completeness',
    'metrics_1min',
    (COUNT(*) > 0),
    COUNT(*),
    1,
    jsonb_build_object('window', 'all_time')
FROM metrics_1min;

INSERT INTO data_quality_checks (check_name, check_type, table_name, passed, metric_value, threshold_value, details)
SELECT
    'heartbeat_session_summary_non_empty',
    'completeness',
    'session_summary',
    (COUNT(*) > 0),
    COUNT(*),
    1,
    jsonb_build_object('window', 'all_time')
FROM session_summary;

INSERT INTO data_quality_checks (check_name, check_type, table_name, passed, metric_value, threshold_value, details)
SELECT
    'heartbeat_freshness_metrics_1min',
    'timeliness',
    'metrics_1min',
    (EXTRACT(EPOCH FROM (NOW() - COALESCE(MAX(window_end), NOW()))) <= 600),
    EXTRACT(EPOCH FROM (NOW() - COALESCE(MAX(window_end), NOW())))::NUMERIC,
    600,
    jsonb_build_object('unit', 'seconds')
FROM metrics_1min;

-- 2) Pipeline monitoring heartbeat (hourly overwrite)
--
-- pipeline_monitoring is an append-only event log, not a current-state table,
-- so multiple writers (this heartbeat + every Airflow DAG) are intentional:
-- each writer only ever inserts rows under its own job_name, never updates
-- another writer's rows.
DELETE FROM pipeline_monitoring
WHERE job_name = 'postgres_maintenance_heartbeat'
  AND start_time >= date_trunc('hour', NOW());

INSERT INTO pipeline_monitoring (
    job_name,
    job_type,
    start_time,
    end_time,
    status,
    records_processed,
    metadata
)
SELECT
    'postgres_maintenance_heartbeat',
    'batch_job',
    date_trunc('hour', NOW()),
    NOW(),
    'success',
    (
      SELECT COALESCE(COUNT(*), 0) FROM metrics_1min WHERE window_start >= NOW() - INTERVAL '1 hour'
    ),
    jsonb_build_object('source', 'maintenance.sql', 'interval_seconds', 60);

-- 3) Retention — keep disk usage bounded
DELETE FROM data_quality_checks WHERE check_timestamp < NOW() - INTERVAL '14 days';
DELETE FROM pipeline_monitoring WHERE start_time < NOW() - INTERVAL '14 days';
DELETE FROM metrics_5min WHERE window_start < NOW() - INTERVAL '14 days';
DELETE FROM metrics_1min WHERE window_start < NOW() - INTERVAL '3 days';
DELETE FROM session_summary WHERE start_time < NOW() - INTERVAL '3 days';

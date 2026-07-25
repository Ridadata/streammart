-- ============================================================================
-- StreamMart PostgreSQL Database Initialization
-- ============================================================================

-- Note: This script runs against the 'streammart' database (set via POSTGRES_DB).
-- The 'airflow' database is created automatically by 00_create_airflow_db.sql,
-- which runs first (docker-entrypoint-initdb.d executes scripts in alphabetical
-- order, and that file is prefixed 00_ specifically to guarantee this).

-- Connect to streammart database (this is default based on POSTGRES_DB env var)
-- \c streammart;

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- ============================================================================
-- NOTE ON RAW EVENTS
-- Raw, unaggregated events are NOT stored in Postgres. They live in MinIO as
-- partitioned Parquet (s3a://raw-events/events/year=/month=/day=/event_type=),
-- written by src/spark_jobs/raw_event_writer.py. Postgres holds only
-- aggregated/derived tables. There is deliberately no `events_raw` table here.
-- ============================================================================

-- ============================================================================
-- METRICS TABLES - 1 MINUTE WINDOWS
-- Aggregated metrics computed by Spark every minute
-- ============================================================================

CREATE TABLE IF NOT EXISTS metrics_1min (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    unique_sessions BIGINT NOT NULL DEFAULT 0,
    unique_users BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_1min_window_start ON metrics_1min(window_start DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_1min_event_type ON metrics_1min(event_type);
CREATE UNIQUE INDEX IF NOT EXISTS uq_metrics_1min_window_event ON metrics_1min(window_start, event_type);

COMMENT ON TABLE metrics_1min IS '1-minute windowed aggregations from Spark Streaming';

-- ============================================================================
-- METRICS TABLES - 5 MINUTE WINDOWS
-- Rolled up from 1-minute windows for longer-term trending
-- ============================================================================

CREATE TABLE IF NOT EXISTS metrics_5min (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    unique_sessions BIGINT NOT NULL DEFAULT 0,
    unique_users BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metrics_5min_window_start ON metrics_5min(window_start DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_5min_event_type ON metrics_5min(event_type);
CREATE UNIQUE INDEX IF NOT EXISTS uq_metrics_5min_window_event ON metrics_5min(window_start, event_type);

COMMENT ON TABLE metrics_5min IS '5-minute windowed aggregations rolled up from 1-min metrics';

-- ============================================================================
-- SESSION SUMMARY TABLE
-- Sessionized user behavior with conversion tracking
-- ============================================================================

CREATE TABLE IF NOT EXISTS session_summary (
    session_id TEXT NOT NULL,
    user_id TEXT,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    duration_seconds INT NOT NULL,
    total_events INT NOT NULL DEFAULT 0,
    pageview_count INT NOT NULL DEFAULT 0,
    product_click_count INT NOT NULL DEFAULT 0,
    add_to_cart_count INT NOT NULL DEFAULT 0,
    converted BOOLEAN NOT NULL DEFAULT FALSE,
    revenue DECIMAL(10, 2) DEFAULT 0.00,
    device_type TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_session_summary_session_id ON session_summary(session_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_session_summary_session_id ON session_summary(session_id);
CREATE INDEX IF NOT EXISTS idx_session_summary_user_id ON session_summary(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_session_summary_start_time ON session_summary(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_session_summary_converted ON session_summary(converted);
CREATE INDEX IF NOT EXISTS idx_session_summary_revenue ON session_summary(revenue DESC) WHERE revenue > 0;

COMMENT ON TABLE session_summary IS 'Per-session KPIs and conversion tracking';

-- ============================================================================
-- PRODUCT PERFORMANCE TABLE
-- Aggregated product-level metrics
-- ============================================================================

CREATE TABLE IF NOT EXISTS product_performance (
    product_id TEXT NOT NULL,
    date DATE NOT NULL,
    product_name TEXT,
    product_category TEXT,
    view_count BIGINT NOT NULL DEFAULT 0,
    click_count BIGINT NOT NULL DEFAULT 0,
    add_to_cart_count BIGINT NOT NULL DEFAULT 0,
    purchase_count BIGINT NOT NULL DEFAULT 0,
    revenue DECIMAL(12, 2) DEFAULT 0.00,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (product_id, date)
);

CREATE INDEX IF NOT EXISTS idx_product_performance_date ON product_performance(date DESC);
CREATE INDEX IF NOT EXISTS idx_product_performance_category ON product_performance(product_category);
CREATE INDEX IF NOT EXISTS idx_product_performance_revenue ON product_performance(revenue DESC);

COMMENT ON TABLE product_performance IS 'Daily product-level performance metrics';

-- ============================================================================
-- DAILY REVENUE TABLE
-- High-level daily business metrics (populated by Airflow)
-- ============================================================================

CREATE TABLE IF NOT EXISTS daily_revenue (
    date DATE PRIMARY KEY,
    total_revenue DECIMAL(12, 2) NOT NULL DEFAULT 0.00,
    total_orders INT NOT NULL DEFAULT 0,
    unique_customers INT NOT NULL DEFAULT 0,
    avg_order_value DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    conversion_rate DECIMAL(5, 4) NOT NULL DEFAULT 0.0000,
    cart_abandonment_rate DECIMAL(5, 4) NOT NULL DEFAULT 0.0000,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_daily_revenue_date ON daily_revenue(date DESC);

COMMENT ON TABLE daily_revenue IS 'Daily business KPIs - populated by Airflow DAG';

-- ============================================================================
-- DAILY SUMMARY TABLE
-- Comprehensive daily KPIs computed by the batch Airflow DAG (2 AM)
-- Combines event counts from metrics_1min with session stats from session_summary
-- ============================================================================

CREATE TABLE IF NOT EXISTS daily_summary (
    date DATE PRIMARY KEY,
    total_events BIGINT NOT NULL DEFAULT 0,
    pageview_events BIGINT NOT NULL DEFAULT 0,
    product_click_events BIGINT NOT NULL DEFAULT 0,
    add_to_cart_events BIGINT NOT NULL DEFAULT 0,
    purchase_events BIGINT NOT NULL DEFAULT 0,
    abandonment_events BIGINT NOT NULL DEFAULT 0,
    unique_sessions INT NOT NULL DEFAULT 0,
    total_revenue DECIMAL(12, 2) NOT NULL DEFAULT 0.00,
    conversion_rate DECIMAL(5, 2) NOT NULL DEFAULT 0.00,
    avg_session_duration_sec DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE daily_summary IS 'Daily aggregate KPIs computed by Airflow batch DAG';

-- ============================================================================
-- PRODUCT DAILY PERFORMANCE TABLE
-- Per-product daily metrics computed by the batch Airflow DAG
-- Separate from streaming product_performance for Lambda architecture separation
-- ============================================================================

CREATE TABLE IF NOT EXISTS product_daily_performance (
    date DATE NOT NULL,
    product_id TEXT NOT NULL,
    total_clicks BIGINT NOT NULL DEFAULT 0,
    total_purchases BIGINT NOT NULL DEFAULT 0,
    total_revenue DECIMAL(12, 2) NOT NULL DEFAULT 0.00,
    conversion_rate DECIMAL(5, 2) NOT NULL DEFAULT 0.00,
    PRIMARY KEY (date, product_id)
);

CREATE INDEX IF NOT EXISTS idx_product_daily_perf_date ON product_daily_performance(date DESC);

COMMENT ON TABLE product_daily_performance IS 'Batch-computed daily product KPIs from Airflow';

-- ============================================================================
-- DATA QUALITY CHECKS TABLE
-- Track data quality metrics over time
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_quality_checks (
    check_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    check_name TEXT NOT NULL,
    check_type TEXT NOT NULL, -- 'completeness', 'accuracy', 'timeliness', 'consistency'
    table_name TEXT NOT NULL,
    check_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    passed BOOLEAN NOT NULL,
    metric_value DECIMAL(10, 2),
    threshold_value DECIMAL(10, 2),
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dq_checks_timestamp ON data_quality_checks(check_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_dq_checks_table ON data_quality_checks(table_name);
CREATE INDEX IF NOT EXISTS idx_dq_checks_passed ON data_quality_checks(passed);

COMMENT ON TABLE data_quality_checks IS 'Data quality check results from Airflow';

-- ============================================================================
-- PIPELINE MONITORING TABLE
-- Track pipeline job executions and health
-- ============================================================================

CREATE TABLE IF NOT EXISTS pipeline_monitoring (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_name TEXT NOT NULL,
    job_type TEXT NOT NULL, -- 'spark_streaming', 'airflow_dag', 'batch_job'
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ,
    status TEXT NOT NULL, -- 'running', 'success', 'failed'
    records_processed BIGINT,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_monitoring_job_name ON pipeline_monitoring(job_name);
CREATE INDEX IF NOT EXISTS idx_pipeline_monitoring_start_time ON pipeline_monitoring(start_time DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_monitoring_status ON pipeline_monitoring(status);

COMMENT ON TABLE pipeline_monitoring IS 'Pipeline job execution tracking';

-- ============================================================================
-- MATERIALIZED VIEWS FOR DASHBOARD QUERIES
-- ============================================================================

-- Top products by revenue (last 7 days)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_products_7d AS
SELECT
    product_id,
    product_name,
    product_category,
    SUM(revenue) as total_revenue,
    SUM(purchase_count) as total_purchases,
    ROUND(SUM(revenue) / NULLIF(SUM(purchase_count), 0), 2) as avg_price
FROM product_performance
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY product_id, product_name, product_category
ORDER BY total_revenue DESC
LIMIT 50;

CREATE UNIQUE INDEX IF NOT EXISTS mv_top_products_7d_idx ON mv_top_products_7d(product_id);

-- ============================================================================
-- FUNCTIONS & PROCEDURES
-- ============================================================================

-- Function to refresh all materialized views
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS void AS $$
BEGIN
        REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_products_7d;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION refresh_all_materialized_views IS 'Refresh all materialized views - called by Airflow';

-- ============================================================================
-- GRANTS
-- ============================================================================

-- Grant permissions to application user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO streammart_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO streammart_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO streammart_user;

-- ============================================================================
-- INITIAL DATA / SEED DATA
-- ============================================================================

-- Insert initial monitoring record
INSERT INTO pipeline_monitoring (job_name, job_type, start_time, status, metadata)
VALUES (
    'database_initialization',
    'admin',
    NOW(),
    'success',
    '{"version": "1.0.0", "initialized_at": "2026-02-26"}'::jsonb
);

-- ============================================================================
-- VACUUM & ANALYZE
-- ============================================================================

VACUUM ANALYZE;

-- ============================================================================
-- END OF INITIALIZATION
-- ============================================================================

\echo 'StreamMart database initialization completed successfully!'

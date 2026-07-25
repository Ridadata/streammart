-- Keep these tables populated from streaming outputs.
-- Runs every minute via postgres-rollup service.

-- 1) Roll up metrics_1min to metrics_5min (last 12h)
DELETE FROM metrics_5min
WHERE window_start >= NOW() - INTERVAL '12 hours';

INSERT INTO metrics_5min (
    window_start,
    window_end,
    event_type,
    count,
    unique_sessions,
    unique_users,
    created_at,
    updated_at
)
SELECT
    date_bin('5 minutes', window_start, TIMESTAMP '2001-01-01') AS window_start,
    date_bin('5 minutes', window_start, TIMESTAMP '2001-01-01') + INTERVAL '5 minutes' AS window_end,
    event_type,
    SUM(count) AS count,
    SUM(unique_sessions) AS unique_sessions,
    SUM(unique_users) AS unique_users,
    NOW() AS created_at,
    NOW() AS updated_at
FROM metrics_1min
WHERE window_start >= NOW() - INTERVAL '12 hours'
GROUP BY 1, 2, 3;

-- 2) Populate daily_revenue from session_summary
INSERT INTO daily_revenue (
    date,
    total_revenue,
    total_orders,
    unique_customers,
    avg_order_value,
    conversion_rate,
    cart_abandonment_rate,
    updated_at
)
SELECT
    CURRENT_DATE,
    COALESCE(SUM(revenue), 0.00) AS total_revenue,
    COUNT(*) FILTER (WHERE converted = TRUE) AS total_orders,
    COUNT(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL) AS unique_customers,
    COALESCE(ROUND(SUM(revenue) / NULLIF(COUNT(*) FILTER (WHERE converted = TRUE), 0), 2), 0.00) AS avg_order_value,
    COALESCE(ROUND((COUNT(*) FILTER (WHERE converted = TRUE)::NUMERIC / NULLIF(COUNT(*), 0))::NUMERIC, 4), 0.0000) AS conversion_rate,
    COALESCE(ROUND((1 - (COUNT(*) FILTER (WHERE converted = TRUE)::NUMERIC / NULLIF(COUNT(*), 0)))::NUMERIC, 4), 0.0000) AS cart_abandonment_rate,
    NOW()
FROM session_summary
ON CONFLICT (date)
DO UPDATE SET
    total_revenue = EXCLUDED.total_revenue,
    total_orders = EXCLUDED.total_orders,
    unique_customers = EXCLUDED.unique_customers,
    avg_order_value = EXCLUDED.avg_order_value,
    conversion_rate = EXCLUDED.conversion_rate,
    cart_abandonment_rate = EXCLUDED.cart_abandonment_rate,
    updated_at = NOW();

-- 3) Populate product_performance with synthetic aggregate row (keeps table active)
INSERT INTO product_performance (
    product_id,
    date,
    product_name,
    product_category,
    view_count,
    click_count,
    add_to_cart_count,
    purchase_count,
    revenue,
    updated_at
)
SELECT
    'all-products' AS product_id,
    CURRENT_DATE AS date,
    'All Products' AS product_name,
    'all' AS product_category,
    COALESCE(SUM(pageview_count), 0),
    COALESCE(SUM(product_click_count), 0),
    COALESCE(SUM(add_to_cart_count), 0),
    COUNT(*) FILTER (WHERE converted = TRUE),
    COALESCE(SUM(revenue), 0.00),
    NOW()
FROM session_summary
ON CONFLICT (product_id, date)
DO UPDATE SET
    view_count = EXCLUDED.view_count,
    click_count = EXCLUDED.click_count,
    add_to_cart_count = EXCLUDED.add_to_cart_count,
    purchase_count = EXCLUDED.purchase_count,
    revenue = EXCLUDED.revenue,
    updated_at = NOW();

-- 4) Data quality checks snapshot (hourly overwrite)
DELETE FROM data_quality_checks
WHERE check_name IN ('metrics_1min_non_empty', 'session_summary_non_empty', 'freshness_metrics_1min')
  AND check_timestamp >= date_trunc('hour', NOW());

INSERT INTO data_quality_checks (check_name, check_type, table_name, passed, metric_value, threshold_value, details)
SELECT
    'metrics_1min_non_empty',
    'completeness',
    'metrics_1min',
    (COUNT(*) > 0),
    COUNT(*),
    1,
    jsonb_build_object('window', 'all_time')
FROM metrics_1min;

INSERT INTO data_quality_checks (check_name, check_type, table_name, passed, metric_value, threshold_value, details)
SELECT
    'session_summary_non_empty',
    'completeness',
    'session_summary',
    (COUNT(*) > 0),
    COUNT(*),
    1,
    jsonb_build_object('window', 'all_time')
FROM session_summary;

INSERT INTO data_quality_checks (check_name, check_type, table_name, passed, metric_value, threshold_value, details)
SELECT
    'freshness_metrics_1min',
    'timeliness',
    'metrics_1min',
    (EXTRACT(EPOCH FROM (NOW() - COALESCE(MAX(window_end), NOW()))) <= 600),
    EXTRACT(EPOCH FROM (NOW() - COALESCE(MAX(window_end), NOW())))::NUMERIC,
    600,
    jsonb_build_object('unit', 'seconds')
FROM metrics_1min;

-- 5) Pipeline monitoring heartbeat (hourly overwrite)
DELETE FROM pipeline_monitoring
WHERE job_name = 'postgres_rollup_heartbeat'
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
    'postgres_rollup_heartbeat',
    'batch_job',
    date_trunc('hour', NOW()),
    NOW(),
    'success',
    (
      SELECT COALESCE(COUNT(*), 0) FROM metrics_1min WHERE window_start >= NOW() - INTERVAL '1 hour'
    ),
    jsonb_build_object('source', 'rollups.sql', 'interval_seconds', 60);

-- 6) Light retention to avoid disk growth
DELETE FROM data_quality_checks WHERE check_timestamp < NOW() - INTERVAL '14 days';
DELETE FROM pipeline_monitoring WHERE start_time < NOW() - INTERVAL '14 days';
DELETE FROM metrics_5min WHERE window_start < NOW() - INTERVAL '14 days';
DELETE FROM metrics_1min WHERE window_start < NOW() - INTERVAL '3 days';
DELETE FROM session_summary WHERE start_time < NOW() - INTERVAL '3 days';

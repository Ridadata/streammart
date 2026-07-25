"""
Shared pytest fixtures for the StreamMart test suite.

Path setup: the Spark jobs, simulator, and utils modules under src/ are
standalone scripts (no package __init__.py, no setup.py) meant to be run
directly via spark-submit or python — not imported as an installed package.
To unit-test their pure transformation functions, we add each source
directory straight onto sys.path so tests can do e.g.
`from window_aggregator import parse_events`.

Env var guard: several Spark job modules (window_aggregator.py,
session_tracker.py, revenue_aggregator.py, raw_event_writer.py) read
required config (POSTGRES_PASSWORD, MINIO_ACCESS_KEY, MINIO_SECRET_KEY) at
*import* time and raise RuntimeError immediately if missing — intentional,
so a misconfigured deployment fails loudly instead of connecting with
useless defaults (see CLAUDE.md coding standards). Unit tests only exercise
the pure DataFrame transform functions, never the actual Postgres/MinIO
connections, so we set harmless dummy values before any test module can
trigger those imports.
"""

import os
import sys

os.environ.setdefault('POSTGRES_PASSWORD', 'unit-test-placeholder-not-a-real-credential')
os.environ.setdefault('MINIO_ACCESS_KEY', 'unit-test-placeholder-not-a-real-credential')
os.environ.setdefault('MINIO_SECRET_KEY', 'unit-test-placeholder-not-a-real-credential')

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _subdir in ('spark_jobs', 'simulator', 'utils'):
    _path = os.path.join(_REPO_ROOT, 'src', _subdir)
    if _path not in sys.path:
        sys.path.insert(0, _path)

import pytest


@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped local SparkSession for unit-testing pure DataFrame
    transformations (parse_events, compute_*, sessionize_events, etc.)
    None of these functions call anything streaming-specific except
    withWatermark()/session_window(), both of which are valid (effectively
    no-ops for state eviction purposes) on static/batch DataFrames too — so
    we can call the exact same production functions with a plain
    createDataFrame() instead of a Kafka source, no streaming query needed.
    """
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .appName("streammart-unit-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

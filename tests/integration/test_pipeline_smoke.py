"""
Integration smoke tests — require a running StreamMart stack.

    docker compose up -d
    python -m pytest tests/integration -v

These skip automatically (not fail) if Postgres/MinIO aren't reachable, so a
plain `python -m pytest` (unit tests only, no Docker required) is never
broken by this file — see tests/unit/ for the tests that always run.

Deliberately thin: reuses src/utils/postgres_client.py and minio_client.py
(the same clients scripts/validate_data.py uses) rather than reimplementing
connection logic here.
"""

import os

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def postgres_client():
    from postgres_client import get_postgres_client

    client = get_postgres_client()
    try:
        client.execute_query("SELECT 1", dict_cursor=False)
    except Exception as e:
        pytest.skip(f"Postgres not reachable — run `docker compose up -d` first ({e})")
    return client


@pytest.fixture(scope="module")
def minio_client():
    from minio_client import get_minio_client

    try:
        client = get_minio_client()
        client.list_buckets()
    except Exception as e:
        pytest.skip(f"MinIO not reachable — run `docker compose up -d` first ({e})")
    return client


# Mirrors sql/init_postgres.sql. Deliberately excludes `events_raw`, which
# does not and should not exist — see test_events_raw_table_does_not_exist.
REQUIRED_TABLES = [
    'metrics_1min',
    'metrics_5min',
    'session_summary',
    'product_performance',
    'daily_revenue',
    'daily_summary',
    'product_daily_performance',
    'data_quality_checks',
    'pipeline_monitoring',
]


class TestSchema:
    def test_all_required_tables_exist(self, postgres_client):
        missing = [t for t in REQUIRED_TABLES if not postgres_client.table_exists(t)]
        assert not missing, f"Missing tables: {missing}"

    def test_events_raw_table_does_not_exist(self, postgres_client):
        """
        Regression guard: several real bugs (three Airflow DAGs, and this
        repo's own validate_data.py) were written against a table that was
        drafted in a schema-file comment but never actually created — raw
        events live in MinIO as Parquet, not in Postgres. If this table
        exists, something added it back without updating everything that
        currently assumes its absence (see CLAUDE.md).
        """
        assert not postgres_client.table_exists('events_raw')


class TestMinioDataLake:
    def test_raw_events_bucket_exists(self, minio_client):
        bucket = os.getenv('MINIO_BUCKET', 'raw-events')
        assert minio_client.bucket_exists(bucket)

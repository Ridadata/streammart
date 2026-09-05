"""
Airflow DAG integrity tests.

Two of the four DAGs in this project previously failed on every single task,
every single run, because they referenced Postgres columns that don't exist
(see ENGINEERING.md for the full history). None of that was caught by any test,
because there wasn't one — a broken query only fails once Airflow actually
tries to run the task against a real database, which never happened in CI.

This file cannot catch a broken *query* (that requires a real Postgres —
see tests/integration/), but it catches everything one level up: a DAG file
that doesn't even parse, a cyclic task graph, duplicate task ids, and —
via source inspection — direct regressions of the exact bugs already fixed
once (wrong connection id, a column name that never existed). Cheap test,
real signal.
"""

import importlib.util
import inspect
import os

import pytest

pytest.importorskip("airflow")
pytest.importorskip("airflow.providers.postgres")

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DAGS_DIR = os.path.join(_REPO_ROOT, 'src', 'airflow_dags')

DAG_FILES = {
    'batch_daily_processing_dag.py': 'streammart_daily_batch_processing',
    'daily_summary_dag.py': 'streammart_daily_summary',
    'data_quality_dag.py': 'streammart_data_quality',
    'pipeline_health_check_dag.py': 'streammart_pipeline_health_check',
}

# Patterns that caused real production failures (see git history / ENGINEERING.md).
# events_raw / .timestamp / .properties: queried a table that was drafted in
# a DDL comment but never created — raw events live in MinIO, not Postgres.
# is_converted: the real column on session_summary is `converted`.
# postgres_default: airflow-init only provisions a connection named
# streammart_postgres; nothing named postgres_default exists.
FORBIDDEN_PATTERNS = {
    'events_raw': "queries a table that was never created — raw events live in MinIO, not Postgres",
    'is_converted': "session_summary's real column is `converted`, not `is_converted`",
    'postgres_default': "airflow-init only provisions the `streammart_postgres` connection",
}


def _load_dag_module(filename):
    path = os.path.join(DAGS_DIR, filename)
    module_name = f"streammart_dag_under_test_{filename[:-3]}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("filename,expected_dag_id", DAG_FILES.items())
def test_dag_file_parses_and_constructs(filename, expected_dag_id):
    module = _load_dag_module(filename)
    assert hasattr(module, 'dag'), f"{filename} does not define a top-level `dag` object"
    assert module.dag.dag_id == expected_dag_id


@pytest.mark.parametrize("filename", DAG_FILES.keys())
def test_dag_task_graph_has_no_cycles(filename):
    module = _load_dag_module(filename)
    # Raises AirflowDagCycleException if the task graph has a cycle.
    module.dag.topological_sort()


@pytest.mark.parametrize("filename", DAG_FILES.keys())
def test_dag_task_ids_are_unique(filename):
    module = _load_dag_module(filename)
    task_ids = [t.task_id for t in module.dag.tasks]
    assert len(task_ids) == len(set(task_ids))


@pytest.mark.parametrize("filename", DAG_FILES.keys())
def test_dag_has_at_least_one_task(filename):
    module = _load_dag_module(filename)
    assert len(module.dag.tasks) > 0


@pytest.mark.parametrize("filename", DAG_FILES.keys())
def test_dag_source_contains_no_known_broken_patterns(filename):
    module = _load_dag_module(filename)
    source = inspect.getsource(module)
    for pattern, reason in FORBIDDEN_PATTERNS.items():
        assert pattern not in source, f"{filename} contains '{pattern}': {reason}"


def test_batch_processing_dag_does_not_use_uncontrolled_catchup():
    """
    Regression guard for the backfill-storm bug: catchup=True combined with
    a start_date months in the past meant unpausing the DAG fired well over
    a hundred backfill runs at once. See batch_daily_processing_dag.py's
    DAG-definition comment for the intentional replacement (explicit,
    bounded `airflow dags backfill` when a real backfill is wanted).
    """
    module = _load_dag_module('batch_daily_processing_dag.py')
    assert module.dag.catchup is False

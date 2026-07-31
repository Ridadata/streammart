"""
Unit tests for window_aggregator.py's pure transformation logic.

compute_windowed_aggregations() is parametrized by window size (see module
docstring: this job computes metrics_1min and metrics_5min independently from
the raw event stream, rather than deriving one from the other) — these tests
exercise it at both granularities to make sure that design actually works,
not just that it looks right on paper.
"""

import json
from datetime import datetime, timedelta

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import Row

from window_aggregator import parse_events, compute_windowed_aggregations


def _kafka_row(topic, value_dict, ts):
    return Row(topic=topic, value=json.dumps(value_dict), timestamp=ts)


class TestParseEvents:
    def test_extracts_minimal_fields_needed_for_aggregation(self, spark):
        ts = datetime(2026, 6, 15, 10, 0, 0)
        payload = {
            "event_id": "e1", "session_id": "s1", "user_id": "u1",
            "timestamp": int(ts.timestamp() * 1000),
        }
        df = spark.createDataFrame([_kafka_row("events.pageview", payload, ts)])

        result = parse_events(df).collect()[0]

        assert result.event_id == "e1"
        assert result.session_id == "s1"
        assert result.user_id == "u1"
        assert result.event_type == "pageview"

    def test_drops_rows_with_null_event_time(self, spark):
        payload = {"event_id": "e2", "session_id": "s2", "user_id": "u2"}
        df = spark.createDataFrame([_kafka_row("events.pageview", payload, datetime(2026, 6, 15))])

        result = parse_events(df).collect()

        assert len(result) == 0


class TestComputeWindowedAggregations:
    def test_counts_events_and_uniques_per_type(self, spark):
        base = datetime(2026, 6, 15, 10, 0, 5)
        rows = [
            Row(event_id="e1", session_id="s1", user_id="u1", event_time=base, event_type="pageview"),
            Row(event_id="e2", session_id="s1", user_id="u1", event_time=base + timedelta(seconds=10), event_type="pageview"),
            Row(event_id="e3", session_id="s2", user_id="u2", event_time=base + timedelta(seconds=5), event_type="purchase"),
        ]
        df = spark.createDataFrame(rows)

        result = compute_windowed_aggregations(df, "1 minute", "2 minutes").collect()
        by_type = {r.event_type: r for r in result}

        assert by_type["pageview"]["count"] == 2
        assert by_type["pageview"].unique_sessions == 1
        assert by_type["purchase"]["count"] == 1
        assert by_type["purchase"].unique_users == 1

    def test_1min_and_5min_windows_bucket_the_same_events_differently(self, spark):
        """
        The whole point of computing both granularities independently (see
        module docstring on why metrics_5min is not SUM()'d from
        metrics_1min): two events 2 minutes apart land in different 1-minute
        windows but the same 5-minute window.
        """
        base = datetime(2026, 6, 15, 10, 0, 5)
        rows = [
            Row(event_id="e1", session_id="s1", user_id="u1", event_time=base, event_type="pageview"),
            Row(event_id="e2", session_id="s1", user_id="u1", event_time=base + timedelta(minutes=2), event_type="pageview"),
        ]
        df = spark.createDataFrame(rows)

        result_1min = compute_windowed_aggregations(df, "1 minute", "2 minutes").collect()
        result_5min = compute_windowed_aggregations(df, "5 minutes", "2 minutes").collect()

        assert len(result_1min) == 2  # two separate 1-minute windows
        assert len(result_5min) == 1  # same 5-minute window
        assert result_5min[0]["count"] == 2

    def test_unique_sessions_deduplicates_within_a_window(self, spark):
        base = datetime(2026, 6, 15, 10, 0, 5)
        rows = [
            Row(event_id="e1", session_id="s1", user_id="u1", event_time=base, event_type="pageview"),
            Row(event_id="e2", session_id="s1", user_id="u1", event_time=base + timedelta(seconds=20), event_type="pageview"),
            Row(event_id="e3", session_id="s1", user_id="u1", event_time=base + timedelta(seconds=40), event_type="pageview"),
        ]
        df = spark.createDataFrame(rows)

        result = compute_windowed_aggregations(df, "1 minute", "2 minutes").collect()[0]

        assert result["count"] == 3
        assert result.unique_sessions == 1

"""
Unit tests for session_tracker.py's pure transformation logic.

sessionize_events() uses session_window(), which Spark supports on both
static (batch) and streaming DataFrames — so it's fully testable here with
plain createDataFrame() input, even though write_to_postgres() (not tested
here) requires "append" output mode specifically for the streaming query
(see that function's docstring for why "update" mode isn't an option for
session-window aggregations).
"""

import json
from datetime import datetime, timedelta

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import Row

from session_tracker import parse_events, sessionize_events


def _kafka_row(topic, value_dict):
    return Row(topic=topic, value=json.dumps(value_dict))


class TestParseEvents:
    def test_extracts_revenue_from_purchase_events(self, spark):
        ts_ms = int(datetime(2026, 6, 15, 10, 0, 0).timestamp() * 1000)
        payload = {
            "event_id": "e1", "session_id": "s1", "user_id": "u1",
            "timestamp": ts_ms, "device_type": "DESKTOP",
            "total": 49.99, "order_id": "ORD-1",
        }
        df = spark.createDataFrame([_kafka_row("events.purchase", payload)])

        result = parse_events(df).collect()[0]

        assert result.session_id == "s1"
        assert result.event_type == "purchase"
        assert result.revenue == 49.99

    def test_non_purchase_events_have_zero_revenue(self, spark):
        ts_ms = int(datetime(2026, 6, 15, 10, 0, 0).timestamp() * 1000)
        payload = {
            "event_id": "e2", "session_id": "s1", "user_id": "u1",
            "timestamp": ts_ms, "device_type": "MOBILE",
        }
        df = spark.createDataFrame([_kafka_row("events.pageview", payload)])

        result = parse_events(df).collect()[0]

        assert result.revenue == 0.0

    def test_drops_events_with_no_session_id(self, spark):
        ts_ms = int(datetime(2026, 6, 15, 10, 0, 0).timestamp() * 1000)
        payload = {"event_id": "e3", "user_id": "u1", "timestamp": ts_ms, "device_type": "MOBILE"}
        df = spark.createDataFrame([_kafka_row("events.pageview", payload)])

        result = parse_events(df).collect()

        assert len(result) == 0


class TestSessionizeEvents:
    def test_computes_session_kpis_and_conversion_flag(self, spark):
        base = datetime(2026, 6, 15, 10, 0, 0)
        rows = [
            Row(session_id="s1", user_id="u1", event_type="pageview", event_time=base, device_type="DESKTOP", revenue=0.0),
            Row(session_id="s1", user_id="u1", event_type="product_click", event_time=base + timedelta(minutes=1), device_type="DESKTOP", revenue=0.0),
            Row(session_id="s1", user_id="u1", event_type="add_to_cart", event_time=base + timedelta(minutes=2), device_type="DESKTOP", revenue=0.0),
            Row(session_id="s1", user_id="u1", event_type="purchase", event_time=base + timedelta(minutes=3), device_type="DESKTOP", revenue=99.99),
        ]
        df = spark.createDataFrame(rows)

        result = sessionize_events(df).collect()

        assert len(result) == 1
        session = result[0]
        assert session.session_id == "s1"
        assert session.total_events == 4
        assert session.pageview_count == 1
        assert session.add_to_cart_count == 1
        assert session.converted is True
        assert float(session.revenue) == 99.99

    def test_session_without_purchase_is_not_converted(self, spark):
        base = datetime(2026, 6, 15, 10, 0, 0)
        rows = [
            Row(session_id="s2", user_id="u2", event_type="pageview", event_time=base, device_type="MOBILE", revenue=0.0),
            Row(session_id="s2", user_id="u2", event_type="add_to_cart", event_time=base + timedelta(minutes=1), device_type="MOBILE", revenue=0.0),
        ]
        df = spark.createDataFrame(rows)

        result = sessionize_events(df).collect()[0]

        assert result.converted is False
        assert float(result.revenue) == 0.0

    def test_gap_over_30_minutes_splits_into_separate_sessions(self, spark):
        base = datetime(2026, 6, 15, 10, 0, 0)
        rows = [
            Row(session_id="s3", user_id="u3", event_type="pageview", event_time=base, device_type="DESKTOP", revenue=0.0),
            Row(session_id="s3", user_id="u3", event_type="pageview", event_time=base + timedelta(minutes=45), device_type="DESKTOP", revenue=0.0),
        ]
        df = spark.createDataFrame(rows)

        result = sessionize_events(df).collect()

        # 45-minute gap exceeds the 30-minute session_window gap duration
        assert len(result) == 2

    def test_activity_within_30_minutes_stays_one_session(self, spark):
        base = datetime(2026, 6, 15, 10, 0, 0)
        rows = [
            Row(session_id="s4", user_id="u4", event_type="pageview", event_time=base, device_type="DESKTOP", revenue=0.0),
            Row(session_id="s4", user_id="u4", event_type="pageview", event_time=base + timedelta(minutes=20), device_type="DESKTOP", revenue=0.0),
        ]
        df = spark.createDataFrame(rows)

        result = sessionize_events(df).collect()

        assert len(result) == 1
        assert result[0].total_events == 2

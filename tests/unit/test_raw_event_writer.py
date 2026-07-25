"""
Unit tests for raw_event_writer.py's pure transformation logic.

Only parse_events() is tested here — create_spark_session(), read_kafka_stream()
and write_to_minio() all touch external systems (Kafka, MinIO) or streaming-only
APIs and aren't meaningfully unit-testable without a running stack (that's what
tests/integration/ is for).
"""

import json
from datetime import datetime

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import Row

from raw_event_writer import parse_events


def _kafka_row(topic, key, value_dict, ts):
    return Row(topic=topic, key=key, value=json.dumps(value_dict), timestamp=ts)


class TestParseEvents:
    def test_extracts_event_type_from_topic_name(self, spark):
        ts = datetime(2026, 6, 15, 10, 30, 0)
        payload = {
            "event_id": "e1", "session_id": "session-1",
            "timestamp": int(ts.timestamp() * 1000), "device_type": "DESKTOP",
        }
        df = spark.createDataFrame([_kafka_row("events.pageview", "session-1", payload, ts)])

        result = parse_events(df).collect()

        assert len(result) == 1
        assert result[0].event_type == "pageview"

    def test_derives_date_partition_columns_from_event_timestamp(self, spark):
        ts = datetime(2026, 6, 15, 10, 30, 0)
        payload = {
            "event_id": "e2", "session_id": "session-2",
            "timestamp": int(ts.timestamp() * 1000), "device_type": "MOBILE",
        }
        df = spark.createDataFrame([_kafka_row("events.purchase", "session-2", payload, ts)])

        result = parse_events(df).collect()[0]

        assert result.year == 2026
        assert result.month == 6
        assert result.day == 15

    def test_preserves_full_raw_json_payload_unmodified(self, spark):
        """
        This is the whole point of the raw layer's schema-on-read design
        (see module docstring in raw_event_writer.py): the exact original
        payload must survive intact, including fields the writer itself
        never inspects.
        """
        ts = datetime(2026, 6, 15, 10, 30, 0)
        payload = {
            "event_id": "e3", "session_id": "session-3",
            "timestamp": int(ts.timestamp() * 1000), "device_type": "TABLET",
            "product_id": "ELEC-001", "product_price": 599.99,
            "a_field_the_writer_has_never_heard_of": "still here",
        }
        df = spark.createDataFrame([_kafka_row("events.add_to_cart", "session-3", payload, ts)])

        result = parse_events(df).collect()[0]

        assert json.loads(result.value_str) == payload

    def test_drops_events_missing_a_timestamp(self, spark):
        payload = {"event_id": "e4", "session_id": "session-4", "device_type": "DESKTOP"}
        df = spark.createDataFrame([_kafka_row("events.pageview", "session-4", payload, datetime(2026, 6, 15))])

        result = parse_events(df).collect()

        assert len(result) == 0

    def test_session_id_comes_from_kafka_key_not_payload(self, spark):
        ts = datetime(2026, 6, 15, 10, 30, 0)
        payload = {
            "event_id": "e5", "session_id": "session-in-payload",
            "timestamp": int(ts.timestamp() * 1000), "device_type": "DESKTOP",
        }
        df = spark.createDataFrame([_kafka_row("events.pageview", "session-from-kafka-key", payload, ts)])

        result = parse_events(df).collect()[0]

        assert result.session_id == "session-from-kafka-key"

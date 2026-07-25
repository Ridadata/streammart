"""
Unit tests for revenue_aggregator.py's pure transformation logic.

compute_product_performance() specifically tests the bounded-state fix (see
module docstring): the grouping key includes a real window() column, and
these tests confirm that grouping still produces correct per-product,
per-day aggregates — not just that it "doesn't blow up state" (which isn't
observable from a single-batch unit test anyway; that fix matters for a
long-running streaming query, not something a unit test can directly assert).
"""

import json
from datetime import datetime

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import Row

from revenue_aggregator import parse_purchase_events, compute_product_performance


class TestParsePurchaseEvents:
    def test_explodes_one_row_per_line_item(self, spark):
        ts_ms = int(datetime(2026, 6, 15, 10, 0, 0).timestamp() * 1000)
        payload = {
            "event_id": "e1", "session_id": "s1", "user_id": "u1", "timestamp": ts_ms,
            "order_id": "ORD-1",
            "items": [
                {"product_id": "P1", "product_name": "Widget", "product_category": "Cat",
                 "quantity": 2, "unit_price": 10.0, "total_price": 20.0},
                {"product_id": "P2", "product_name": "Gadget", "product_category": "Cat",
                 "quantity": 1, "unit_price": 5.0, "total_price": 5.0},
            ],
            "subtotal": 25.0, "tax": 2.0, "shipping": 0.0, "total": 27.0,
            "payment_method": "CREDIT_CARD",
        }
        df = spark.createDataFrame([Row(value=json.dumps(payload))])

        result = parse_purchase_events(df).collect()

        assert len(result) == 2
        product_ids = {r.product_id for r in result}
        assert product_ids == {"P1", "P2"}
        assert all(r.order_id == "ORD-1" for r in result)

    def test_item_revenue_maps_to_total_price(self, spark):
        ts_ms = int(datetime(2026, 6, 15, 10, 0, 0).timestamp() * 1000)
        payload = {
            "event_id": "e2", "session_id": "s2", "user_id": "u2", "timestamp": ts_ms,
            "order_id": "ORD-2",
            "items": [
                {"product_id": "P1", "product_name": "Widget", "product_category": "Cat",
                 "quantity": 3, "unit_price": 10.0, "total_price": 30.0},
            ],
            "subtotal": 30.0, "tax": 0.0, "shipping": 0.0, "total": 30.0,
            "payment_method": "PAYPAL",
        }
        df = spark.createDataFrame([Row(value=json.dumps(payload))])

        result = parse_purchase_events(df).collect()[0]

        assert result.item_revenue == 30.0
        assert result.quantity == 3


class TestComputeProductPerformance:
    def test_aggregates_revenue_and_units_across_orders(self, spark):
        ts = datetime(2026, 6, 15, 10, 0, 0)
        rows = [
            Row(order_id="ORD-1", user_id="u1", purchase_time=ts, order_total=27.0,
                product_id="P1", product_name="Widget", product_category="Cat",
                quantity=2, unit_price=10.0, item_revenue=20.0),
            Row(order_id="ORD-2", user_id="u2", purchase_time=ts, order_total=15.0,
                product_id="P1", product_name="Widget", product_category="Cat",
                quantity=1, unit_price=10.0, item_revenue=10.0),
        ]
        df = spark.createDataFrame(rows)

        result = compute_product_performance(df).collect()

        assert len(result) == 1
        row = result[0]
        assert row.product_id == "P1"
        assert row.purchase_count == 3  # sum of quantity across both orders
        assert float(row.revenue) == 30.0
        assert row.date == ts.date()

    def test_different_products_produce_separate_rows(self, spark):
        ts = datetime(2026, 6, 15, 10, 0, 0)
        rows = [
            Row(order_id="ORD-1", user_id="u1", purchase_time=ts, order_total=15.0,
                product_id="P1", product_name="Widget", product_category="Cat",
                quantity=1, unit_price=10.0, item_revenue=10.0),
            Row(order_id="ORD-1", user_id="u1", purchase_time=ts, order_total=15.0,
                product_id="P2", product_name="Gadget", product_category="Cat",
                quantity=1, unit_price=5.0, item_revenue=5.0),
        ]
        df = spark.createDataFrame(rows)

        result = compute_product_performance(df).collect()

        assert len(result) == 2
        product_ids = {r.product_id for r in result}
        assert product_ids == {"P1", "P2"}

    def test_same_product_different_days_produces_separate_rows(self, spark):
        """
        This is the direct behavioral test of the bounded-state fix: the
        grouping key is window(purchase_time, "1 day") + product columns,
        not just product columns, so the same product on two different days
        must NOT collapse into a single aggregated row.
        """
        day1 = datetime(2026, 6, 15, 10, 0, 0)
        day2 = datetime(2026, 6, 16, 10, 0, 0)
        rows = [
            Row(order_id="ORD-1", user_id="u1", purchase_time=day1, order_total=10.0,
                product_id="P1", product_name="Widget", product_category="Cat",
                quantity=1, unit_price=10.0, item_revenue=10.0),
            Row(order_id="ORD-2", user_id="u2", purchase_time=day2, order_total=10.0,
                product_id="P1", product_name="Widget", product_category="Cat",
                quantity=1, unit_price=10.0, item_revenue=10.0),
        ]
        df = spark.createDataFrame(rows)

        result = compute_product_performance(df).collect()

        assert len(result) == 2
        dates = {r.date for r in result}
        assert dates == {day1.date(), day2.date()}

"""
Unit tests for Consumer A (Status Tracker).
Tests in-memory state tracking, lifecycle transitions, eviction upon delivery,
and graceful handling of malformed messages.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import consumer_status


@pytest.fixture(autouse=True)
def reset_consumer_state():
    """Resets in-memory active orders before each test."""
    with consumer_status.state_lock:
        consumer_status.active_orders.clear()
    yield
    with consumer_status.state_lock:
        consumer_status.active_orders.clear()


def test_order_lifecycle_and_eviction(capsys):
    """
    Asserts that an order is added to active_orders, updated through intermediate states,
    and evicted upon reaching DELIVERED.
    """
    order_id = "ORD-101"
    base_payload = {
        "order_id": order_id,
        "customer_name": "Maya Lin",
        "restaurant": "Tokyo Ramen & Izakaya",
        "items": ["Tonkotsu Pork Ramen"],
        "status": "PLACED",
        "timestamp": "2026-09-22T15:00:00Z",
        "estimated_delivery_minutes": 25
    }

    # 1. PLACED
    consumer_status.process_order_event(base_payload)
    with consumer_status.state_lock:
        assert order_id in consumer_status.active_orders
        assert consumer_status.active_orders[order_id]["status"] == "PLACED"

    # 2. CONFIRMED
    base_payload["status"] = "CONFIRMED"
    consumer_status.process_order_event(base_payload)
    with consumer_status.state_lock:
        assert consumer_status.active_orders[order_id]["status"] == "CONFIRMED"

    # 3. PREPARING
    base_payload["status"] = "PREPARING"
    consumer_status.process_order_event(base_payload)
    with consumer_status.state_lock:
        assert consumer_status.active_orders[order_id]["status"] == "PREPARING"

    # 4. OUT_FOR_DELIVERY
    base_payload["status"] = "OUT_FOR_DELIVERY"
    consumer_status.process_order_event(base_payload)
    with consumer_status.state_lock:
        assert consumer_status.active_orders[order_id]["status"] == "OUT_FOR_DELIVERY"

    # 5. DELIVERED -> Eviction
    base_payload["status"] = "DELIVERED"
    consumer_status.process_order_event(base_payload)
    with consumer_status.state_lock:
        assert order_id not in consumer_status.active_orders, "Delivered order must be evicted from active_orders"

    captured = capsys.readouterr()
    assert f"[COMPLETE] {order_id} | Tokyo Ramen & Izakaya | ~25 min" in captured.out


def test_malformed_event_handling():
    """Asserts that malformed payloads (missing order_id or status) are safely skipped."""
    # Missing order_id
    consumer_status.process_order_event({"status": "PLACED", "restaurant": "Test"})
    # Missing status
    consumer_status.process_order_event({"order_id": "ORD-999", "restaurant": "Test"})
    # Empty payload
    consumer_status.process_order_event({})

    with consumer_status.state_lock:
        assert len(consumer_status.active_orders) == 0

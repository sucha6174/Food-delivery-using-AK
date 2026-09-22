"""
Unit tests for Consumer B (Analytics Engine).
Tests independent metrics aggregation per restaurant and status type.
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import consumer_analytics


@pytest.fixture(autouse=True)
def reset_analytics_state():
    """Resets analytics metrics before each test."""
    with consumer_analytics.metrics_lock:
        consumer_analytics.orders_by_restaurant.clear()
        consumer_analytics.events_by_status.clear()
        consumer_analytics.total_events = 0
    yield
    with consumer_analytics.metrics_lock:
        consumer_analytics.orders_by_restaurant.clear()
        consumer_analytics.events_by_status.clear()
        consumer_analytics.total_events = 0


def test_record_event_aggregation(capsys):
    """Asserts that events properly increment status and restaurant counters."""
    events = [
        {"order_id": "ORD-001", "restaurant": "Burger Craft Artisan Lab", "status": "PLACED"},
        {"order_id": "ORD-001", "restaurant": "Burger Craft Artisan Lab", "status": "CONFIRMED"},
        {"order_id": "ORD-002", "restaurant": "Tokyo Ramen & Izakaya", "status": "PLACED"},
        {"order_id": "ORD-002", "restaurant": "Tokyo Ramen & Izakaya", "status": "PREPARING"},
    ]

    for ev in events:
        consumer_analytics.record_event(ev)

    with consumer_analytics.metrics_lock:
        assert consumer_analytics.total_events == 4
        assert consumer_analytics.orders_by_restaurant["Burger Craft Artisan Lab"] == 2
        assert consumer_analytics.orders_by_restaurant["Tokyo Ramen & Izakaya"] == 2
        assert consumer_analytics.events_by_status["PLACED"] == 2
        assert consumer_analytics.events_by_status["CONFIRMED"] == 1
        assert consumer_analytics.events_by_status["PREPARING"] == 1

    # Verify report output formatting
    consumer_analytics.print_analytics_report()
    captured = capsys.readouterr()
    assert "REAL-TIME ORDER ANALYTICS SNAPSHOT" in captured.out
    assert "Total Events Received: 4" in captured.out
    assert "Burger Craft Artisan Lab" in captured.out
    assert "PLACED" in captured.out


def test_record_malformed_event_ignored():
    """Asserts that events missing restaurant or status are safely skipped."""
    consumer_analytics.record_event({"order_id": "ORD-999"})
    consumer_analytics.record_event({"status": "PLACED"})
    consumer_analytics.record_event({})

    with consumer_analytics.metrics_lock:
        assert consumer_analytics.total_events == 0

"""
Unit tests for the Order Event Producer.
Tests payload structure, status validation, CLI argument defaults, and Kafka publishing.
"""

import os
import sys
from unittest.mock import MagicMock, patch
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from producer import create_order_payload, publish_event, simulate_orders, parse_args
from config import ORDER_STATUS_FLOW


def test_create_order_payload_structure():
    """Asserts that create_order_payload generates all required schema fields."""
    payload = create_order_payload(
        order_id="ORD-001",
        customer_name="Alex Mercer",
        restaurant="Bella Napoli Trattoria",
        items=["Margherita Pizza DOC", "Cold Brew Iced Coffee"],
        status="PLACED",
        estimated_delivery_minutes=25,
        timestamp="2026-09-22T15:00:00Z"
    )

    assert payload["order_id"] == "ORD-001"
    assert payload["customer_name"] == "Alex Mercer"
    assert payload["restaurant"] == "Bella Napoli Trattoria"
    assert payload["items"] == ["Margherita Pizza DOC", "Cold Brew Iced Coffee"]
    assert payload["status"] == "PLACED"
    assert payload["timestamp"] == "2026-09-22T15:00:00Z"
    assert payload["estimated_delivery_minutes"] == 25


def test_invalid_status_raises_error():
    """Asserts that an unrecognized order status raises a ValueError."""
    with pytest.raises(ValueError, match="Invalid status"):
        create_order_payload(
            order_id="ORD-001",
            customer_name="Test Customer",
            restaurant="Test Restaurant",
            items=["Item 1"],
            status="CANCELLED_OR_UNKNOWN",
            estimated_delivery_minutes=20
        )


def test_publish_event_uses_order_id_as_key(capsys):
    """
    Asserts that publish_event uses order_id as the message key
    and prints the [SENT] confirmation format.
    """
    mock_producer = MagicMock()
    mock_future = MagicMock()
    mock_future.get.return_value = MagicMock(topic="order-events", partition=1, offset=42)
    mock_producer.send.return_value = mock_future

    message = {
        "order_id": "ORD-042",
        "customer_name": "Tyler Brooks",
        "restaurant": "Burger Craft Artisan Lab",
        "items": ["Classic Double Smashed Burger"],
        "status": "CONFIRMED",
        "timestamp": "2026-09-22T15:00:00Z",
        "estimated_delivery_minutes": 30
    }

    success = publish_event(mock_producer, "order-events", message)
    assert success is True

    # Check that send was called with key="ORD-042"
    mock_producer.send.assert_called_once_with(
        "order-events",
        key="ORD-042",
        value=message
    )

    captured = capsys.readouterr()
    assert "[SENT] ORD-042 -> CONFIRMED" in captured.out


def test_simulate_orders_event_count(capsys):
    """
    Asserts that simulating 3 orders publishes exactly 15 messages (3 orders * 5 statuses)
    and that all 5 statuses are generated in strict sequence.
    """
    mock_producer = MagicMock()
    mock_future = MagicMock()
    mock_future.get.return_value = MagicMock(topic="order-events", partition=0, offset=1)
    mock_producer.send.return_value = mock_future

    # Zero delay for rapid unit test execution
    simulate_orders(num_orders=3, delay_min=0, delay_max=0, producer=mock_producer)

    assert mock_producer.send.call_count == 15

    captured = capsys.readouterr()
    lines = [line for line in captured.out.strip().split("\n") if line.startswith("[SENT]")]
    assert len(lines) == 15

    # Check lifecycle progression for ORD-001
    ord1_lines = [l for l in lines if "ORD-001" in l]
    assert len(ord1_lines) == 5
    for idx, expected_status in enumerate(ORDER_STATUS_FLOW):
        assert f"ORD-001 -> {expected_status}" in ord1_lines[idx]


def test_cli_argument_default():
    """Asserts that CLI arguments default to 10 orders."""
    with patch("sys.argv", ["producer.py"]):
        args = parse_args()
        assert args.orders == 10

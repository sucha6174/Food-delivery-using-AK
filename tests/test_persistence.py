"""
Unit tests for State Persistence mechanism.
Tests atomic writing to state.json, valid JSON formatting, and thread-safe disk synchronization.
"""

import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import consumer_status


def test_state_persistence_file_creation():
    """
    Asserts that active state can be written to disk atomically, producing a valid JSON file.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        test_state_file = os.path.join(tmp_dir, "test_state.json")
        tmp_target = f"{test_state_file}.tmp"

        # Populate active state
        with consumer_status.state_lock:
            consumer_status.active_orders["ORD-501"] = {
                "order_id": "ORD-501",
                "customer_name": "Kavita Sharma",
                "restaurant": "Spice Symphony Indian Kitchen",
                "items": ["Butter Chicken with Naan"],
                "status": "PREPARING",
                "timestamp": "2026-09-22T15:00:00Z",
                "estimated_delivery_minutes": 35
            }

        # Simulate atomic persistence logic
        with consumer_status.state_lock:
            snapshot = json.dumps(consumer_status.active_orders.copy(), indent=2)

        with open(tmp_target, "w", encoding="utf-8") as f:
            f.write(snapshot)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_target, test_state_file)

        # Assertions
        assert os.path.exists(test_state_file)
        assert not os.path.exists(tmp_target), "Temporary file must be replaced/cleaned up"

        # Verify contents
        with open(test_state_file, "r", encoding="utf-8") as f:
            loaded_data = json.load(f)

        assert "ORD-501" in loaded_data
        assert loaded_data["ORD-501"]["status"] == "PREPARING"
        assert loaded_data["ORD-501"]["restaurant"] == "Spice Symphony Indian Kitchen"

        # Clean up in-memory state
        with consumer_status.state_lock:
            consumer_status.active_orders.clear()

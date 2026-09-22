"""
Unit tests for Flask REST API endpoints in Consumer A.
Tests GET /state, GET /health, and dashboard static file serving.
"""

import os
import sys
import json
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from consumer_status import app, active_orders, state_lock


@pytest.fixture
def client():
    """Provides a test client for the Flask application."""
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture(autouse=True)
def clean_orders():
    """Clears state before and after each test."""
    with state_lock:
        active_orders.clear()
    yield
    with state_lock:
        active_orders.clear()


def test_get_state_empty(client):
    """Asserts that GET /state returns an empty JSON object when no orders are active."""
    response = client.get("/state")
    assert response.status_code == 200
    assert response.is_json
    data = response.get_json()
    assert data == {}


def test_get_state_with_active_orders(client):
    """Asserts that GET /state returns active orders keyed by order_id."""
    with state_lock:
        active_orders["ORD-201"] = {
            "order_id": "ORD-201",
            "customer_name": "Brianna Patel",
            "restaurant": "Mediterranean Breeze",
            "items": ["Greek Village Salad with Feta"],
            "status": "CONFIRMED",
            "timestamp": "2026-09-22T15:00:00Z",
            "estimated_delivery_minutes": 20
        }

    response = client.get("/state")
    assert response.status_code == 200
    assert response.content_type == "application/json"
    data = response.get_json()
    assert "ORD-201" in data
    assert data["ORD-201"]["status"] == "CONFIRMED"
    assert data["ORD-201"]["restaurant"] == "Mediterranean Breeze"


def test_get_health(client):
    """Asserts that GET /health returns service status and count of active orders."""
    with state_lock:
        active_orders["ORD-301"] = {"order_id": "ORD-301", "status": "PLACED"}
        active_orders["ORD-302"] = {"order_id": "ORD-302", "status": "PREPARING"}

    response = client.get("/health")
    assert response.status_code == 200
    data = response.get_json()
    assert data["status"] == "healthy"
    assert data["service"] == "status-tracker"
    assert data["active_orders_count"] == 2


def test_serve_dashboard(client):
    """Asserts that root path GET / serves the HTML dashboard."""
    response = client.get("/")
    assert response.status_code == 200
    assert b"FlashBite" in response.data
    assert b"Apache Kafka" in response.data

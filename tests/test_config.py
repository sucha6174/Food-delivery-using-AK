"""
Unit tests for configuration module.
Verifies environment variable loading and sensible default values.
"""

import os
import sys
import importlib

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import config


def test_default_config():
    """Asserts that default configuration values are properly set."""
    assert config.TOPIC_ORDER_EVENTS == "order-events"
    assert config.TOPIC_PARTITIONS == 3
    assert config.TOPIC_RETENTION_MS == 3600000
    assert config.STATUS_GROUP_ID == "status-tracker"
    assert config.ANALYTICS_GROUP_ID == "analytics"
    assert config.PORT == 5000
    assert config.PERSIST_INTERVAL == 10
    assert config.ANALYTICS_INTERVAL == 15
    assert len(config.ORDER_STATUS_FLOW) == 5


def test_env_override(monkeypatch):
    """Asserts that environment variables override configuration defaults."""
    monkeypatch.setenv("KAFKA_BROKER", "192.168.1.100:9092")
    monkeypatch.setenv("PORT", "8080")
    monkeypatch.setenv("PERSIST_INTERVAL", "5")
    
    # Reload module to test environment variable reading
    importlib.reload(config)
    
    assert config.KAFKA_BROKER == "192.168.1.100:9092"
    assert config.PORT == 8080
    assert config.PERSIST_INTERVAL == 5

    # Reset environment variables
    monkeypatch.undo()
    importlib.reload(config)

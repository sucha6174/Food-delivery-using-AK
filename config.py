"""
Configuration module for the Food Delivery Real-Time Order Tracking System.
Loads application parameters from environment variables with safe, sensible defaults.
"""

import os
import logging

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_ORDER_EVENTS = os.getenv("TOPIC_ORDER_EVENTS", "order-events")
TOPIC_PARTITIONS = int(os.getenv("TOPIC_PARTITIONS", "3"))
TOPIC_RETENTION_MS = int(os.getenv("TOPIC_RETENTION_MS", "3600000"))  # 1 hour

# Consumer Groups
STATUS_GROUP_ID = os.getenv("STATUS_GROUP_ID", "status-tracker")
ANALYTICS_GROUP_ID = os.getenv("ANALYTICS_GROUP_ID", "analytics")

# Server / API Configuration
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "5000"))

# Intervals (seconds)
PERSIST_INTERVAL = int(os.getenv("PERSIST_INTERVAL", "10"))
ANALYTICS_INTERVAL = int(os.getenv("ANALYTICS_INTERVAL", "15"))
DEFAULT_ORDERS = int(os.getenv("DEFAULT_ORDERS", "10"))

# State Persistence File (default located in project root)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.getenv("STATE_FILE", os.path.join(BASE_DIR, "state.json"))

# Order Lifecycle Sequence
ORDER_STATUS_FLOW = [
    "PLACED",
    "CONFIRMED",
    "PREPARING",
    "OUT_FOR_DELIVERY",
    "DELIVERED"
]

def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    """Configures and returns a standardized console logger."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(level)
    return logger

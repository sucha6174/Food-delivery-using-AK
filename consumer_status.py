"""
Consumer A: Order Status Tracker & State API Service.
Tracks active orders from the order-events Kafka topic, maintains thread-safe in-memory state,
periodically persists state to state.json using atomic file replacement, and serves
the /state REST API and live status dashboard frontend.
"""

import json
import os
import sys
import threading
import time
from typing import Dict, Any, Optional
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config import (
    KAFKA_BROKER,
    TOPIC_ORDER_EVENTS,
    STATUS_GROUP_ID,
    HOST,
    PORT,
    PERSIST_INTERVAL,
    STATE_FILE,
    setup_logger
)

logger = setup_logger("StatusTracker")

# Flask application setup
frontend_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")
app = Flask(__name__, static_folder=frontend_dir)
CORS(app)

# In-memory state storage and synchronization lock
active_orders: Dict[str, Dict[str, Any]] = {}
state_lock = threading.Lock()
shutdown_event = threading.Event()


def get_consumer(broker_address: str, group_id: str, topic: str, max_retries: int = 5, retry_delay: float = 2.0) -> KafkaConsumer:
    """
    Initializes a KafkaConsumer with retry logic.
    Handles startup delays of the Kafka broker.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=[broker_address],
                group_id=group_id,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=1000
            )
            logger.info("Connected Consumer A to broker at %s (Group: %s, Topic: %s)", broker_address, group_id, topic)
            return consumer
        except Exception as exc:
            attempt += 1
            logger.warning(
                "Attempt %d/%d: Failed to connect consumer to %s (%s). Retrying in %.1fs...",
                attempt, max_retries, broker_address, exc, retry_delay
            )
            time.sleep(retry_delay)

    logger.error("Consumer A failed to connect to Kafka broker after %d attempts.", max_retries)
    raise ConnectionError(f"Consumer could not connect to Kafka at {broker_address}")


def process_order_event(payload: dict) -> None:
    """
    Processes an individual order event message and updates in-memory active_orders state.
    Safely executed under state_lock or called by consumer loop.
    """
    order_id = payload.get("order_id")
    status = payload.get("status")

    if not order_id or not status:
        logger.warning("Skipping malformed event without order_id or status: %s", payload)
        return

    with state_lock:
        old_status = active_orders[order_id]["status"] if order_id in active_orders else None
        
        if status == "DELIVERED":
            restaurant = payload.get("restaurant", "Unknown Restaurant")
            est_mins = payload.get("estimated_delivery_minutes", 30)
            print(f"[COMPLETE] {order_id} | {restaurant} | ~{est_mins} min", flush=True)
            if order_id in active_orders:
                del active_orders[order_id]
        else:
            if old_status:
                print(f"[UPDATE] {order_id}: {old_status} -> {status}", flush=True)
            else:
                print(f"[UPDATE] {order_id}: -> {status}", flush=True)
            active_orders[order_id] = payload


def consume_events(broker: str = KAFKA_BROKER, group_id: str = STATUS_GROUP_ID, topic: str = TOPIC_ORDER_EVENTS):
    """
    Kafka consumption loop.
    Polls messages from the order-events topic and dispatches to process_order_event.
    """
    logger.info("Starting Consumer A event loop...")
    consumer = None
    try:
        consumer = get_consumer(broker, group_id, topic)
        while not shutdown_event.is_set():
            try:
                message_batch = consumer.poll(timeout_ms=1000)
                for tp, messages in message_batch.items():
                    for message in messages:
                        try:
                            payload = message.value
                            process_order_event(payload)
                        except Exception as e:
                            logger.error("Error processing message at offset %s: %s", message.offset, e)
            except Exception as loop_err:
                if not shutdown_event.is_set():
                    logger.error("Error in consumer poll loop: %s", loop_err)
                    time.sleep(1.0)
    except Exception as e:
        logger.error("Consumer A encountered fatal error: %s", e)
    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed.")


def save_state_to_disk(state_file_path: str = STATE_FILE):
    """
    Periodically saves current active_orders dictionary to disk every PERSIST_INTERVAL seconds.
    Uses atomic file replacement (writing to a temp file and renaming via os.replace)
    to prevent file corruption in case of unexpected crashes or interruptions.
    """
    logger.info("Persistence thread started (Interval: %ds, Target: %s)", PERSIST_INTERVAL, state_file_path)
    tmp_path = f"{state_file_path}.tmp"
    
    while not shutdown_event.is_set():
        shutdown_event.wait(PERSIST_INTERVAL)
        if shutdown_event.is_set():
            break

        try:
            with state_lock:
                snapshot = json.dumps(active_orders.copy(), indent=2)

            # Atomic write pattern
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(snapshot)
                f.flush()
                os.fsync(f.fileno())

            os.replace(tmp_path, state_file_path)
        except Exception as e:
            logger.error("Error saving state to %s: %s", state_file_path, e)


# --- REST API Endpoints ---

@app.route("/state", methods=["GET"])
def get_state():
    """
    Returns the current dictionary of active orders.
    Uses state_lock to ensure thread-safety against concurrent consumer writes.
    """
    with state_lock:
        # Shallow copy dictionary under lock before serialization to prevent RuntimeError
        current_state = active_orders.copy()
    return jsonify(current_state), 200


@app.route("/health", methods=["GET"])
def get_health():
    """Healthcheck endpoint reporting service status and active order counts."""
    with state_lock:
        active_count = len(active_orders)
    return jsonify({
        "status": "healthy",
        "service": "status-tracker",
        "active_orders_count": active_count,
        "timestamp": time.time()
    }), 200


@app.route("/", methods=["GET"])
def serve_dashboard():
    """Serves the frontend live status board."""
    return send_from_directory(frontend_dir, "index.html")


@app.route("/<path:path>", methods=["GET"])
def serve_static(path):
    """Serves frontend static assets (CSS, JS)."""
    return send_from_directory(frontend_dir, path)


def start_service(broker: str = KAFKA_BROKER, port: int = PORT, host: str = HOST):
    """Starts the consumer, persistence worker, and web server."""
    consumer_thread = threading.Thread(
        target=consume_events,
        args=(broker,),
        name="KafkaConsumerThread",
        daemon=True
    )
    consumer_thread.start()

    persistence_thread = threading.Thread(
        target=save_state_to_disk,
        args=(STATE_FILE,),
        name="StatePersistenceThread",
        daemon=True
    )
    persistence_thread.start()

    logger.info("Starting Flask API server on http://%s:%d", host, port)
    app.run(host=host, port=port, debug=False, use_reloader=False)


if __name__ == "__main__":
    try:
        start_service()
    except KeyboardInterrupt:
        logger.info("Shutting down Status Tracker...")
        shutdown_event.set()
        sys.exit(0)

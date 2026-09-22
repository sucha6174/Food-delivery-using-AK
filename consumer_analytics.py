"""
Consumer B: Real-Time Analytics Engine.
Independently consumes order events from the order-events Kafka topic under consumer group 'analytics'.
Aggregates order metrics per restaurant and lifecycle status, periodically outputting
formatted analytics snapshots to the console every 15 seconds.
"""

import collections
import json
import sys
import threading
import time
from typing import Dict
from kafka import KafkaConsumer

from config import (
    KAFKA_BROKER,
    TOPIC_ORDER_EVENTS,
    ANALYTICS_GROUP_ID,
    ANALYTICS_INTERVAL,
    setup_logger
)

logger = setup_logger("AnalyticsEngine")

# Metrics state and synchronization lock
orders_by_restaurant: Dict[str, int] = collections.defaultdict(int)
events_by_status: Dict[str, int] = collections.defaultdict(int)
total_events: int = 0
metrics_lock = threading.Lock()
shutdown_event = threading.Event()


def get_consumer(broker_address: str, group_id: str, topic: str, max_retries: int = 5, retry_delay: float = 2.0) -> KafkaConsumer:
    """
    Initializes an independent KafkaConsumer for Consumer B.
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
            logger.info("Connected Consumer B to broker at %s (Group: %s, Topic: %s)", broker_address, group_id, topic)
            return consumer
        except Exception as exc:
            attempt += 1
            logger.warning(
                "Attempt %d/%d: Failed to connect analytics consumer to %s (%s). Retrying in %.1fs...",
                attempt, max_retries, broker_address, exc, retry_delay
            )
            time.sleep(retry_delay)

    logger.error("Consumer B failed to connect to Kafka broker after %d attempts.", max_retries)
    raise ConnectionError(f"Analytics consumer could not connect to Kafka at {broker_address}")


def record_event(payload: dict) -> None:
    """Updates metrics counters under thread-safe lock."""
    global total_events
    restaurant = payload.get("restaurant")
    status = payload.get("status")

    if not restaurant or not status:
        logger.warning("Skipping event with missing restaurant or status: %s", payload)
        return

    with metrics_lock:
        total_events += 1
        orders_by_restaurant[restaurant] += 1
        events_by_status[status] += 1


def print_analytics_report():
    """Generates and prints a clean, formatted snapshot of real-time metrics."""
    with metrics_lock:
        tot = total_events
        rest_snapshot = dict(orders_by_restaurant)
        status_snapshot = dict(events_by_status)

    separator = "=" * 62
    lines = [
        "",
        separator,
        "               REAL-TIME ORDER ANALYTICS SNAPSHOT         ",
        separator,
        f" Total Events Received: {tot}",
        "-" * 62,
        " [Events by Status]"
    ]
    
    if not status_snapshot:
        lines.append("   (No events received yet)")
    else:
        for st, count in sorted(status_snapshot.items(), key=lambda x: x[0]):
            bar = "=" * min(count, 30)
            lines.append(f"   {st:<18} : {count:>4}  {bar}")

    lines.append("-" * 62)
    lines.append(" [Top Restaurants by Activity]")
    if not rest_snapshot:
        lines.append("   (No restaurant activity recorded yet)")
    else:
        top_rest = sorted(rest_snapshot.items(), key=lambda x: x[1], reverse=True)[:8]
        for name, count in top_rest:
            lines.append(f"   {name:<30} : {count:>3} updates")

    lines.append(separator)
    lines.append("")
    
    print("\n".join(lines), flush=True)


def report_worker(interval: int = ANALYTICS_INTERVAL):
    """Background worker that triggers analytics report snapshots every `interval` seconds."""
    logger.info("Analytics reporting worker started (Interval: %ds)", interval)
    while not shutdown_event.is_set():
        shutdown_event.wait(interval)
        if shutdown_event.is_set():
            break
        print_analytics_report()


def consume_events(broker: str = KAFKA_BROKER, group_id: str = ANALYTICS_GROUP_ID, topic: str = TOPIC_ORDER_EVENTS):
    """Kafka consumption loop for analytics metrics."""
    logger.info("Starting Consumer B event loop...")
    consumer = None
    try:
        consumer = get_consumer(broker, group_id, topic)
        while not shutdown_event.is_set():
            try:
                message_batch = consumer.poll(timeout_ms=1000)
                for tp, messages in message_batch.items():
                    for message in messages:
                        try:
                            record_event(message.value)
                        except Exception as e:
                            logger.error("Error recording event at offset %s: %s", message.offset, e)
            except Exception as loop_err:
                if not shutdown_event.is_set():
                    logger.error("Error in analytics poll loop: %s", loop_err)
                    time.sleep(1.0)
    except Exception as e:
        logger.error("Consumer B encountered fatal error: %s", e)
    finally:
        if consumer:
            consumer.close()
            logger.info("Consumer B Kafka connection closed.")


def start_service(broker: str = KAFKA_BROKER):
    """Starts the reporting thread and starts polling for messages."""
    reporter_thread = threading.Thread(
        target=report_worker,
        name="AnalyticsReporterThread",
        daemon=True
    )
    reporter_thread.start()

    try:
        consume_events(broker)
    except KeyboardInterrupt:
        logger.info("Analytics consumer stopped by user.")
        shutdown_event.set()
        sys.exit(0)


if __name__ == "__main__":
    start_service()

"""
Order Event Producer.
Simulates food delivery order lifecycle events and publishes them to the Kafka topic.
Uses order_id as the message key to guarantee partition-level ordering.
"""

import argparse
import datetime
import json
import random
import sys
import time
from typing import Dict, List, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import (
    KAFKA_BROKER,
    TOPIC_ORDER_EVENTS,
    DEFAULT_ORDERS,
    ORDER_STATUS_FLOW,
    setup_logger
)
from fixtures import RESTAURANTS, CUSTOMERS, MENU_ITEMS

logger = setup_logger("Producer")


def create_order_payload(
    order_id: str,
    customer_name: str,
    restaurant: str,
    items: List[str],
    status: str,
    estimated_delivery_minutes: int,
    timestamp: Optional[str] = None
) -> dict:
    """Constructs a validated message dictionary conforming to the event schema."""
    if status not in ORDER_STATUS_FLOW:
        raise ValueError(f"Invalid status: {status}. Must be one of {ORDER_STATUS_FLOW}")
    
    if not timestamp:
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    return {
        "order_id": order_id,
        "customer_name": customer_name,
        "restaurant": restaurant,
        "items": items,
        "status": status,
        "timestamp": timestamp,
        "estimated_delivery_minutes": estimated_delivery_minutes
    }


def get_producer(broker_address: str, max_retries: int = 5, retry_delay: float = 2.0) -> KafkaProducer:
    """
    Initializes a KafkaProducer with connection retry logic.
    Handles startup delays when the Kafka broker is initializing.
    """
    attempt = 0
    last_exception = None

    while attempt < max_retries:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker_address],
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
                request_timeout_ms=10000
            )
            logger.info("Successfully connected to Kafka broker at %s", broker_address)
            return producer
        except Exception as exc:
            attempt += 1
            last_exception = exc
            logger.warning(
                "Attempt %d/%d: Failed to connect to Kafka at %s (%s). Retrying in %.1fs...",
                attempt, max_retries, broker_address, exc, retry_delay
            )
            time.sleep(retry_delay)

    logger.error("Could not connect to Kafka broker after %d attempts.", max_retries)
    raise ConnectionError(f"Failed to connect to Kafka at {broker_address}: {last_exception}")


def publish_event(
    producer: KafkaProducer,
    topic: str,
    message: dict,
    max_retries: int = 3
) -> bool:
    """
    Publishes an event to Kafka with key=order_id.
    Retries up to max_retries on failure.
    Prints [SENT] {order_id} -> {status} upon successful delivery.
    """
    order_id = message["order_id"]
    status = message["status"]
    
    for attempt in range(1, max_retries + 1):
        try:
            future = producer.send(
                topic,
                key=order_id,
                value=message
            )
            # Block until message is acknowledged to guarantee delivery order
            record_metadata = future.get(timeout=10)
            print(f"[SENT] {order_id} -> {status}", flush=True)
            return True
        except (KafkaError, Exception) as exc:
            logger.warning(
                "Publish attempt %d/%d failed for %s (%s): %s",
                attempt, max_retries, order_id, status, exc
            )
            if attempt < max_retries:
                time.sleep(1.0)
            else:
                logger.error("Failed to publish %s -> %s after %d attempts", order_id, status, max_retries)
                return False


def simulate_orders(
    num_orders: int,
    broker: str = KAFKA_BROKER,
    topic: str = TOPIC_ORDER_EVENTS,
    delay_min: float = 2.0,
    delay_max: float = 5.0,
    producer: Optional[KafkaProducer] = None
):
    """
    Simulates the lifecycle of N food delivery orders.
    Each order progresses sequentially through PLACED -> DELIVERED.
    """
    if producer is None:
        producer = get_producer(broker)

    logger.info("Starting simulation of %d orders...", num_orders)

    # Pre-generate orders
    active_simulations = []
    for i in range(1, num_orders + 1):
        order_id = f"ORD-{i:03d}"
        customer = random.choice(CUSTOMERS)
        restaurant = random.choice(RESTAURANTS)
        item_count = random.randint(1, 4)
        items = random.sample(MENU_ITEMS, item_count)
        est_minutes = random.randint(15, 45)
        active_simulations.append({
            "order_id": order_id,
            "customer": customer,
            "restaurant": restaurant,
            "items": items,
            "est_minutes": est_minutes
        })

    # Sequentially publish lifecycle stages
    for sim in active_simulations:
        for status in ORDER_STATUS_FLOW:
            payload = create_order_payload(
                order_id=sim["order_id"],
                customer_name=sim["customer"],
                restaurant=sim["restaurant"],
                items=sim["items"],
                status=status,
                estimated_delivery_minutes=sim["est_minutes"]
            )
            publish_event(producer, topic, payload)
            
            # Delay between lifecycle stages for realistic simulation
            if status != ORDER_STATUS_FLOW[-1] and delay_max > 0:
                delay = random.uniform(delay_min, delay_max)
                time.sleep(delay)

    producer.flush()
    logger.info("Finished simulating %d orders.", num_orders)


def parse_args():
    parser = argparse.ArgumentParser(description="Food Delivery Kafka Order Event Producer")
    parser.add_argument(
        "--orders",
        type=int,
        default=DEFAULT_ORDERS,
        help=f"Number of orders to simulate (default: {DEFAULT_ORDERS})"
    )
    parser.add_argument(
        "--broker",
        type=str,
        default=KAFKA_BROKER,
        help=f"Kafka broker address (default: {KAFKA_BROKER})"
    )
    parser.add_argument(
        "--delay-min",
        type=float,
        default=2.0,
        help="Minimum delay in seconds between order status updates (default: 2.0)"
    )
    parser.add_argument(
        "--delay-max",
        type=float,
        default=5.0,
        help="Maximum delay in seconds between order status updates (default: 5.0)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    try:
        simulate_orders(
            num_orders=args.orders,
            broker=args.broker,
            delay_min=args.delay_min,
            delay_max=args.delay_max
        )
    except KeyboardInterrupt:
        logger.info("Producer simulation interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.exception("Producer terminated due to error: %s", e)
        sys.exit(1)

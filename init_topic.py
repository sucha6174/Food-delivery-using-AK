"""
Topic Initialization Helper Script.
Connects to Kafka and ensures the 'order-events' topic is created with:
- Exactly 3 partitions
- 1 hour message retention (retention.ms=3600000)
- Replication factor 1
"""

import sys
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import (
    KAFKA_BROKER,
    TOPIC_ORDER_EVENTS,
    TOPIC_PARTITIONS,
    TOPIC_RETENTION_MS,
    setup_logger
)

logger = setup_logger("TopicInit")


def initialize_topic(
    broker: str = KAFKA_BROKER,
    topic_name: str = TOPIC_ORDER_EVENTS,
    partitions: int = TOPIC_PARTITIONS,
    retention_ms: int = TOPIC_RETENTION_MS,
    max_retries: int = 5,
    retry_delay: float = 2.0
):
    """Initializes the Kafka topic with target partitions and retention configuration."""
    attempt = 0
    admin_client = None

    while attempt < max_retries:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=[broker],
                client_id="topic_initializer"
            )
            logger.info("Connected to Kafka admin client at %s", broker)
            break
        except Exception as exc:
            attempt += 1
            logger.warning(
                "Attempt %d/%d: Failed to connect to admin client at %s (%s). Retrying...",
                attempt, max_retries, broker, exc
            )
            time.sleep(retry_delay)

    if not admin_client:
        logger.error("Could not connect to Kafka AdminClient after %d attempts.", max_retries)
        return False

    try:
        topic_list = [
            NewTopic(
                name=topic_name,
                num_partitions=partitions,
                replication_factor=1,
                topic_configs={"retention.ms": str(retention_ms)}
            )
        ]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(
            "Successfully created topic '%s' with %d partitions and retention.ms=%d",
            topic_name, partitions, retention_ms
        )
        return True
    except TopicAlreadyExistsError:
        logger.info("Topic '%s' already exists.", topic_name)
        return True
    except Exception as e:
        logger.error("Failed to create topic '%s': %s", topic_name, e)
        return False
    finally:
        admin_client.close()


if __name__ == "__main__":
    success = initialize_topic()
    sys.exit(0 if success else 1)

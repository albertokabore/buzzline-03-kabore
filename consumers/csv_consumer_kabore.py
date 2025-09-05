"""
json_consumer_kabore.py

Consume JSON messages from a Kafka topic and keep simple author counts.

Example serialized Kafka message:
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON after deserialization:
{"message": "I love Python!", "author": "Eve"}
"""

import os
import json
from collections import defaultdict, deque
from dotenv import load_dotenv

from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

load_dotenv()

# ---------- .env getters ----------

def get_kafka_topic() -> str:
    topic = os.getenv("BUZZ_TOPIC", "buzz_json")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id = os.getenv("BUZZ_CONSUMER_GROUP_ID", "kabore_buzz_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_reset_on_start() -> bool:
    # Set BUZZ_RESET_ON_START=1 to read from beginning each time
    val = os.getenv("BUZZ_RESET_ON_START", "1").strip().lower()
    return val in ("1", "true", "yes", "y")

# ---------- state ----------

author_counts = defaultdict(int)

# ---------- message processing ----------

def process_message(message) -> None:
    """
    message may be bytes or str depending on deserializer settings
    """
    try:
        if isinstance(message, (bytes, bytearray)):
            message = message.decode("utf-8", errors="ignore")

        logger.debug(f"Raw message: {message}")
        data: dict = json.loads(message)
        logger.info(f"Processed JSON message: {data}")

        if not isinstance(data, dict):
            logger.error(f"Expected dict, got {type(data)}")
            return

        author = data.get("author", "unknown")
        author_counts[author] += 1
        logger.info(f"Updated author counts: {dict(author_counts)}")

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON message: {e}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

# ---------- main ----------

def main() -> None:
    logger.info("START json_consumer_kabore")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()

    consumer = create_kafka_consumer(topic, group_id)

    # Force partition assignment and optionally start from beginning
    try:
        consumer.poll(timeout_ms=0)  # trigger assignment on some clients
        assignment = getattr(consumer, "assignment", lambda: [])()
        if assignment and get_reset_on_start():
            consumer.seek_to_beginning(*assignment)
            logger.info("Offsets reset. Reading from beginning of topic.")
    except Exception as e:
        logger.warning(f"Offset reset not applied: {e}")

    logger.info(f"Polling messages from topic '{topic}'")
    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            if not records:
                logger.debug("Waiting for messages...")
                continue
            for _tp, msgs in records.items():
                for rec in msgs:
                    process_message(rec.value)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")
        logger.info("END json_consumer_kabore")

if __name__ == "__main__":
    main()

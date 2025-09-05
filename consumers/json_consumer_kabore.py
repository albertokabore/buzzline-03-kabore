"""
json_consumer_kabore.py

Consume JSON messages from a Kafka topic and process them.

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}
"""

#####################################
# Import Modules
#####################################

# Standard Library
import os
import json
from collections import defaultdict  # count messages per author

# External
from dotenv import load_dotenv

# Local
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "buzzline_json")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "kabore_json_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Data Store to hold author counts
#####################################

# {author: count}
author_counts = defaultdict(int)

#####################################
# Process a single message
#####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka.

    Args:
        message: The JSON message as a string.
    """
    try:
        logger.debug(f"Raw message: {message}")

        message_dict: dict = json.loads(message)
        logger.info(f"Processed JSON message: {message_dict}")

        if isinstance(message_dict, dict):
            author = message_dict.get("author", "unknown")
            author_counts[author] += 1
            logger.info(f"Message received from author: {author}")
            logger.info(f"Updated author counts: {dict(author_counts)}")
        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Main
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads topic and group id from environment.
    - Creates a Kafka consumer via utility.
    - Polls and processes messages forever.
    """
    logger.info("START kabore consumer.")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()

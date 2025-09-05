"""
json_producer_kabore.py

Stream JSON data to a Kafka topic using your kabore producer.

Example JSON message
{"message": "I love Python!", "author": "Eve"}

Example serialized to Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"
"""

# Standard Library
import os
import sys
import time
import pathlib
import json

# Third Party
from dotenv import load_dotenv

# Local Utilities
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

# Load environment
load_dotenv()


# .env getters
def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "buzzline_json")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("BUZZ_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


# Paths
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

DATA_FOLDER: pathlib.Path = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

DATA_FILE: pathlib.Path = DATA_FOLDER.joinpath("buzz.json")
logger.info(f"Data file: {DATA_FILE}")


# Message generator
def generate_messages(file_path: pathlib.Path):
    """
    Read from a JSON file and yield entries one by one in a loop.

    Args:
        file_path: Path to the JSON file.

    Yields:
        dict: One JSON object per yield.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {file_path}")
            with open(file_path, "r") as json_file:
                logger.info(f"Reading data from file: {file_path}")
                json_data = json.load(json_file)

                if not isinstance(json_data, list):
                    raise ValueError(
                        f"Expected a list of JSON objects, got {type(json_data)}."
                    )

                for entry in json_data:
                    if not isinstance(entry, dict):
                        logger.error(f"Skipping non dict entry: {entry}")
                        continue
                    logger.debug(f"Generated JSON: {entry}")
                    yield entry

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in file: {file_path}. Error: {e}")
            sys.exit(2)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)


# Main
def main():
    """
    Entry point for the kabore JSON producer.
    Ensures topic, creates producer, and streams messages from buzz.json.
    """
    logger.info("START kabore producer.")
    verify_services()

    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    if not DATA_FILE.exists():
        logger.error(f"Data file not found: {DATA_FILE}. Exiting.")
        sys.exit(1)

    producer = create_kafka_producer(
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
    )
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting.")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for message_dict in generate_messages(DATA_FILE):
            producer.send(topic, value=message_dict)
            logger.info(f"Sent message to topic '{topic}': {message_dict}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END kabore producer.")


if __name__ == "__main__":
    main()

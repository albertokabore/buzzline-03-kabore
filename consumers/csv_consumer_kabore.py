"""
csv_consumer_kabore.py

Consume JSON messages from a Kafka topic and detect smoker stalls.

Example message:
{"timestamp": "2025-01-11T18:15:00Z", "temperature": 225.0}
"""

# Standard Library
import os
import json
from collections import deque

# External
from dotenv import load_dotenv

# Local
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Load environment variables
load_dotenv()

#####################################
# .env getters
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("SMOKER_TOPIC", "smoker_csv")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("SMOKER_CONSUMER_GROUP_ID", "kabore_smoker_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


def get_stall_threshold() -> float:
    """Fetch stall threshold in Fahrenheit or use default."""
    temp_variation = float(os.getenv("SMOKER_STALL_THRESHOLD_F", 2.0))
    logger.info(f"Max stall temperature range: {temp_variation} F")
    return temp_variation


def get_rolling_window_size() -> int:
    """Fetch rolling window size or use default."""
    window_size = int(os.getenv("SMOKER_ROLLING_WINDOW_SIZE", 5))
    logger.info(f"Rolling window size: {window_size}")
    return window_size


#####################################
# Stall detection
#####################################

def detect_stall(rolling_window_deque: deque) -> bool:
    """
    Detect a temperature stall based on the rolling window.

    Returns True when the range across the window is less than or equal
    to the configured threshold.
    """
    window_size: int = get_rolling_window_size()
    if len(rolling_window_deque) < window_size:
        logger.debug(
            f"Rolling window size: {len(rolling_window_deque)}. Waiting for {window_size}."
        )
        return False

    temp_range = max(rolling_window_deque) - min(rolling_window_deque)
    is_stalled: bool = temp_range <= get_stall_threshold()
    logger.debug(f"Temperature range: {temp_range} F. Stalled: {is_stalled}")
    return is_stalled


#####################################
# Process a single message
#####################################

def process_message(message: str, rolling_window: deque, window_size: int) -> None:
    """
    Process a JSON encoded message and check for stalls.

    message is expected to be a JSON string with keys timestamp and temperature.
    """
    try:
        logger.debug(f"Raw message: {message}")

        data: dict = json.loads(message)
        temperature = data.get("temperature")
        timestamp = data.get("timestamp")
        logger.info(f"Processed JSON message: {data}")

        if temperature is None or timestamp is None:
            logger.error(f"Invalid message format: {message}")
            return

        try:
            temperature = float(temperature)
        except Exception as e:
            logger.error(f"Bad temperature '{temperature}' error {e}")
            return

        rolling_window.append(temperature)

        if detect_stall(rolling_window):
            logger.warning(
                f"STALL DETECTED at {timestamp}: temp stable near {temperature} F over last {window_size} readings."
            )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")


#####################################
# Main
#####################################

def main() -> None:
    """
    Create a consumer and monitor the smoker topic for stalls.
    """
    logger.info("START csv_consumer_kabore")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    window_size = get_rolling_window_size()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'")
    logger.info(f"Rolling window size: {window_size}")

    rolling_window = deque(maxlen=window_size)
    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'")
    try:
        for message in consumer:
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, rolling_window, window_size)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")
        logger.info("END csv_consumer_kabore")


if __name__ == "__main__":
    main()

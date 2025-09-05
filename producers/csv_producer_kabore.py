"""
csv_producer_kabore.py

Stream numeric data from a CSV file as JSON to a Kafka topic.
"""

# Standard Library
import os
import sys
import time
import pathlib
import csv
import json
from datetime import datetime

# Third Party
from dotenv import load_dotenv

# Local Utilities
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
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


def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("SMOKER_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval


#####################################
# Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

DATA_FOLDER = PROJECT_ROOT.joinpath("data")
logger.info(f"Data folder: {DATA_FOLDER}")

DATA_FILE = DATA_FOLDER.joinpath("smoker_temps.csv")
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Message generator
#####################################

def generate_messages(file_path: pathlib.Path):
    """
    Read from a CSV file and yield records as dicts in a loop.

    Args:
        file_path: Path to the CSV file.

    Yields:
        dict: {"timestamp": str, "temperature": float}
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {file_path}")
            with open(file_path, "r", newline="") as csv_file:
                logger.info(f"Reading data from file: {file_path}")
                reader = csv.DictReader(csv_file)

                for row in reader:
                    if "temperature" not in row:
                        logger.error(f"Missing 'temperature' column in row: {row}")
                        continue

                    # Prefer timestamp from CSV if available, else use current UTC
                    ts_raw = row.get("timestamp")
                    if ts_raw:
                        try:
                            # Try to parse common formats then standardize to ISO
                            ts = datetime.fromisoformat(ts_raw)
                            timestamp = ts.isoformat()
                        except Exception:
                            # Fallback if CSV timestamp is not ISO format
                            try:
                                ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
                                timestamp = ts.isoformat()
                            except Exception:
                                timestamp = datetime.utcnow().isoformat()
                                logger.warning(f"Unparsable timestamp '{ts_raw}', using UTC now")
                    else:
                        timestamp = datetime.utcnow().isoformat()

                    try:
                        temperature = float(row["temperature"])
                    except Exception as e:
                        logger.error(f"Bad temperature '{row.get('temperature')}' error {e}")
                        continue

                    message = {"timestamp": timestamp, "temperature": temperature}
                    logger.debug(f"Generated message: {message}")
                    yield message

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)

#####################################
# Main
#####################################

def main():
    """
    Start the kabore CSV producer.
    """
    logger.info("START csv_producer_kabore")
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
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    logger.info(f"Starting message production to topic '{topic}'...")
    try:
        for csv_message in generate_messages(DATA_FILE):
            producer.send(topic, value=csv_message)
            logger.info(f"Sent message to topic '{topic}': {csv_message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")
        logger.info("END csv_producer_kabore")

if __name__ == "__main__":
    main()

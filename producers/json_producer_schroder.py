"""
json_producer_schroder.py

Stream nutrition JSON data (from data/food.json) to a Kafka topic.
"""

import os
import sys
import time
import pathlib
import json
from typing import Generator, Dict, Any

from dotenv import load_dotenv

from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

# Load env
load_dotenv()

# ---- Getters ----
def get_kafka_topic() -> str:
    topic = os.getenv("KAFKA_TOPIC_JSON_SCHRODER", "nutrition_json")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS_JSON_SCHRODER", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

# ---- Paths ----
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

DATA_FOLDER = PROJECT_ROOT / "data"
logger.info(f"Data folder: {DATA_FOLDER}")

DATA_FILE = DATA_FOLDER / "food.json"   # you said your file is named food.json
logger.info(f"Data file: {DATA_FILE}")

# ---- Message generator ----
def generate_messages(file_path: pathlib.Path) -> Generator[Dict[str, Any], None, None]:
    while True:
        try:
            logger.info(f"Opening data file in read mode: {file_path}")
            with open(file_path, "r") as f:
                logger.info(f"Reading data from file: {file_path}")
                data = json.load(f)
                if not isinstance(data, list):
                    raise ValueError("food.json must be a JSON ARRAY of records")
                for entry in data:
                    logger.debug(f"Generated JSON: {entry}")
                    yield entry
        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in {file_path}: {e}")
            sys.exit(2)
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)

# ---- Main ----
def main():
    logger.info("START nutrition producer (schroder).")
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
        for msg in generate_messages(DATA_FILE):
            producer.send(topic, value=msg)
            logger.info(f"Sent message to topic '{topic}': {msg}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close(timeout=None)
        logger.info("Kafka producer closed.")

    logger.info("END nutrition producer (schroder).")

if __name__ == "__main__":
    main()

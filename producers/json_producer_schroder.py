"""
json_producer_nutrition.py

Stream nutrition JSON data to a Kafka topic.

Each JSON message example:
{
    "ts": "2025-09-05T18:30:00Z",
    "meal": "Greek yogurt",
    "protein_g": 23,
    "carb_g": 15,
    "fat_g": 2,
    "kcal": 190,
    "training_day": true
}
"""

#####################################
# Import Modules
#####################################

import os
import sys
import time
import pathlib
import json
from typing import Generator, Dict, Any

from dotenv import load_dotenv

# Local utils
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """
    Topic for nutrition JSON stream.
    """
    topic = os.getenv("KAFKA_TOPIC_JSON_NUTRITION", "nutrition_json")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_message_interval() -> int:
    """
    How many seconds to wait between messages.
    """
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS_JSON_NUTRITION", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

#####################################
# Set up Paths
#####################################

PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

DATA_FOLDER: pathlib.Path = PROJECT_ROOT / "data"
logger.info(f"Data folder: {DATA_FOLDER}")

DATA_FILE: pathlib.Path = DATA_FOLDER / "nutrition.json"
logger.info(f"Data file: {DATA_FILE}")

#####################################
# Message Generator
#####################################

def generate_messages(file_path: pathlib.Path) -> Generator[Dict[str, Any], None, None]:
    """
    Read a JSON array from file and yield entries one by one, repeatedly.

    Args:
        file_path (pathlib.Path): Path to the JSON file.

    Yields:
        dict: Single nutrition record.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {file_path}")
            with open(file_path, "r") as json_file:
                logger.info(f"Reading data from file: {file_path}")
                data: list[Dict[str, Any]] = json.load(json_file)

                if not isinstance(data, list):
                    raise ValueError("nutrition.json must contain a JSON ARRAY of records")

                for entry in data:
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

#####################################
# Main Function
#####################################

def main():
    """
    - Ensures the Kafka topic exists
    - Creates a Kafka producer (JSON serializer)
    - Streams nutrition messages to the topic
    """
    logger.info("START nutrition producer.")
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
        for message_dict in generate_messages(DATA_FILE):
            producer.send(topic, value=message_dict)
            logger.info(f"Sent message to topic '{topic}': {message_dict}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close(timeout=None)
        logger.info("Kafka producer closed.")

    logger.info("END nutrition producer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()

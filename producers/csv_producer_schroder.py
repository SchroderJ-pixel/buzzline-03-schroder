"""
csv_producer_schroder.py

Stream classroom attendance (CSV -> JSON) to a Kafka topic.

CSV columns expected:
timestamp,student,course,status
(status ∈ {present, late, absent})

Each Kafka message (JSON):
{
    "ts": "2025-09-05T13:00:00Z",
    "student": "A. Lee",
    "course": "Algebra",
    "status": "present"
}
"""

# -------------------------------
# Imports
# -------------------------------
import os
import sys
import time
import pathlib
import csv
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

# -------------------------------
# Env
# -------------------------------
load_dotenv()

def get_kafka_topic() -> str:
    topic = os.getenv("KAFKA_TOPIC_CSV_SCHRODER", "attendance_csv")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS_CSV_SCHRODER", 2))
    logger.info(f"Message interval: {interval} seconds")
    return interval

# -------------------------------
# Paths
# -------------------------------
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
logger.info(f"Project root: {PROJECT_ROOT}")

DATA_FOLDER = PROJECT_ROOT / "data"
logger.info(f"Data folder: {DATA_FOLDER}")

DATA_FILE = DATA_FOLDER / "attendance.csv"
logger.info(f"Data file: {DATA_FILE}")

# -------------------------------
# Helpers
# -------------------------------
REQUIRED_COLS = {"timestamp", "student", "course", "status"}
VALID_STATUS = {"present", "late", "absent"}

def _to_iso_utc(ts_str: str | None) -> str:
    """
    Convert CSV timestamp (assumed local or naive) to ISO UTC Z string.
    If empty/missing, use current UTC time.
    """
    if not ts_str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    ts_str = ts_str.strip()
    try:
        # Try parse 'YYYY-mm-dd HH:MM:SS' or 'YYYY-mm-dd HH:MM'
        # If the file already has a 'Z' or offset, datetime.fromisoformat handles it.
        if ts_str.endswith("Z"):
            return ts_str  # already UTC Z
        dt = datetime.fromisoformat(ts_str)
    except Exception:
        # Fallback: best effort parse with seconds optional
        try:
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M")
    # Treat naive as UTC for streaming demo
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

# -------------------------------
# Generator
# -------------------------------
def generate_messages(file_path: pathlib.Path):
    """
    Read attendance CSV and yield JSON records continuously.
    """
    while True:
        try:
            logger.info(f"Opening data file in read mode: {file_path}")
            with open(file_path, "r", newline="") as csv_file:
                logger.info(f"Reading data from file: {file_path}")
                reader = csv.DictReader(csv_file)

                # Validate header
                cols = set(reader.fieldnames or [])
                missing = REQUIRED_COLS - cols
                if missing:
                    logger.error(f"CSV header missing columns: {missing}. Found: {cols}")
                    sys.exit(2)

                for row in reader:
                    status = (row.get("status") or "").strip().lower()
                    if status not in VALID_STATUS:
                        logger.warning(f"Skipping row with invalid status '{status}': {row}")
                        continue

                    msg = {
                        "ts": _to_iso_utc(row.get("timestamp")),
                        "student": (row.get("student") or "").strip(),
                        "course": (row.get("course") or "").strip(),
                        "status": status,
                    }
                    logger.debug(f"Generated JSON: {msg}")
                    yield msg

        except FileNotFoundError:
            logger.error(f"File not found: {file_path}. Exiting.")
            sys.exit(1)
        except SystemExit:
            raise
        except Exception as e:
            logger.error(f"Unexpected error in message generation: {e}")
            sys.exit(3)

# -------------------------------
# Main
# -------------------------------
def main():
    logger.info("START attendance producer (schroder).")
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
        for message in generate_messages(DATA_FILE):
            producer.send(topic, value=message)
            logger.info(f"Sent message to topic '{topic}': {message}")
            time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error during message production: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

    logger.info("END attendance producer (schroder).")

if __name__ == "__main__":
    main()

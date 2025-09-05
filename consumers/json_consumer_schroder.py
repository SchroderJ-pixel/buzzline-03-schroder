"""
json_consumer_schroder.py

Consume nutrition JSON messages and perform real-time analytics/alerts.
"""

import os
import json
from collections import deque, defaultdict
from datetime import datetime
from typing import Any, Dict, Deque, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# Load env
load_dotenv()

# ---- Getters ----
def get_kafka_topic() -> str:
    topic = (
        os.getenv("KAFKA_TOPIC_JSON_SCHRODER")
        or os.getenv("KAFKA_TOPIC_JSON_NUTRITION")
        or os.getenv("BUZZ_TOPIC")
        or "nutrition_json"
    )
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id = (
        os.getenv("KAFKA_GROUP_JSON_SCHRODER")
        or os.getenv("KAFKA_GROUP_JSON_NUTRITION")
        or os.getenv("BUZZ_CONSUMER_GROUP_ID")
        or "nutrition_group"
    )
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


# ---- Config / State ----
LOCAL_TZ = ZoneInfo("America/New_York")
ROLLING_WINDOW_SIZE = 8
PROTEIN_GOAL_G = 160
GOAL_CUTOFF_HOUR = 20  # 8 PM
LATE_EATING_HOUR = 22  # 10 PM
UNDERFUEL_KCAL_2_MEALS = 500
CARB_SPIKE_G = 90

# rolling window of most recent meals
recent: Deque[Dict[str, Any]] = deque(maxlen=ROLLING_WINDOW_SIZE)

# daily aggregates by local date string 'YYYY-MM-DD' -> (protein, carbs, fat, kcal)
daily_totals: Dict[str, Dict[str, float]] = defaultdict(lambda: {"protein_g": 0.0, "carb_g": 0.0, "fat_g": 0.0, "kcal": 0.0})

# keep track of whether we've announced PROTEIN_GOAL for a given local date
protein_goal_announced: Dict[str, bool] = defaultdict(bool)

def parse_ts_utc_to_local(ts: str) -> datetime:
    """
    Parse ISO timestamp with potential trailing 'Z' and convert to America/New_York.
    """
    # Handle 'Z' as +00:00
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt_utc = datetime.fromisoformat(ts)  # aware datetime in UTC
    return dt_utc.astimezone(LOCAL_TZ)

def process_message(message: str) -> None:
    """
    Process a single JSON string from Kafka (utils deserializer gives us str).
    """
    try:
        logger.debug(f"Raw message: {message}")
        record: Dict[str, Any] = json.loads(message)

        # Required fields with basic validation / defaults
        ts = record.get("ts")
        meal = record.get("meal", "unknown")
        protein_g = float(record.get("protein_g", 0))
        carb_g = float(record.get("carb_g", 0))
        fat_g = float(record.get("fat_g", 0))
        kcal = float(record.get("kcal", 0))
        training_day = bool(record.get("training_day", False))

        if not ts:
            logger.warning(f"Skipping record without 'ts': {record}")
            return

        local_dt = parse_ts_utc_to_local(ts)
        local_date = local_dt.date().isoformat()
        local_time_str = local_dt.strftime("%H:%M:%S")

        # Update rolling window and daily totals
        recent.append({"ts": ts, "meal": meal, "protein_g": protein_g, "carb_g": carb_g, "fat_g": fat_g, "kcal": kcal, "training_day": training_day, "local_dt": local_dt})
        daily_totals[local_date]["protein_g"] += protein_g
        daily_totals[local_date]["carb_g"] += carb_g
        daily_totals[local_date]["fat_g"] += fat_g
        daily_totals[local_date]["kcal"] += kcal

        # Info summary
        logger.info(
            f"[{local_date} {local_time_str}] meal='{meal}' "
            f"p={protein_g}g c={carb_g}g f={fat_g}g kcal={kcal} training_day={training_day}"
        )
        logger.info(
            f"Daily so far ({local_date}): "
            f"P={daily_totals[local_date]['protein_g']:.0f}g "
            f"C={daily_totals[local_date]['carb_g']:.0f}g "
            f"F={daily_totals[local_date]['fat_g']:.0f}g "
            f"Kcal={daily_totals[local_date]['kcal']:.0f}"
        )

        # ---- Alerts ----

        # 1) CARB_SPIKE (single meal)
        if carb_g >= CARB_SPIKE_G:
            logger.warning(f"CARB_SPIKE: '{meal}' has {carb_g}g carbs (>= {CARB_SPIKE_G}).")

        # 2) UNDERFUEL_AFTER_WORKOUT (last two meals kcal < 500 on a training day)
        if training_day and len(recent) >= 2:
            kcal_last_two = recent[-1]["kcal"] + recent[-2]["kcal"]
            if kcal_last_two < UNDERFUEL_KCAL_2_MEALS:
                logger.warning(
                    f"UNDERFUEL_AFTER_WORKOUT: last two meals total {kcal_last_two:.0f} kcal (< {UNDERFUEL_KCAL_2_MEALS})."
                )

        # 3) LATE_EATING (>=500 kcal after 10 PM local)
        if local_dt.hour >= LATE_EATING_HOUR and kcal >= 500:
            logger.warning(
                f"LATE_EATING: {kcal:.0f} kcal after {LATE_EATING_HOUR}:00 local (meal='{meal}')."
            )

        # 4) PROTEIN_GOAL (≥160g by 8 PM local). Announce once per day.
        if (local_dt.hour >= GOAL_CUTOFF_HOUR
            and daily_totals[local_date]["protein_g"] >= PROTEIN_GOAL_G
            and not protein_goal_announced[local_date]):
            protein_goal_announced[local_date] = True
            logger.warning(
                f"PROTEIN_GOAL: Reached {daily_totals[local_date]['protein_g']:.0f}g by {GOAL_CUTOFF_HOUR}:00 local. Nice!"
            )

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main() -> None:
    logger.info("START nutrition consumer (schroder).")
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=100)
            if not records:
                continue
            for _tp, batch in records.items():
                for msg in batch:
                    # utils deserializer gives str
                    message_str: str = msg.value
                    logger.debug(f"Received message at offset {msg.offset}: {message_str}")
                    process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")
        logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

if __name__ == "__main__":
    main()

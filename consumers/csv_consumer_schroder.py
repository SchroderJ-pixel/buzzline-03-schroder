"""
csv_consumer_schroder.py

Consume classroom attendance events (JSON from CSV) and raise alerts.

Alerts implemented:
- ATTENDANCE_DROP_CLASS:
    course attendance rate < 85% over the last N events (rolling window)
    and also checked per local day (YYYY-MM-DD).
    (Attendance rate counts 'present' or 'late' as attended.)
- CHRONIC_ABSENCE_STUDENT:
    same student absent >= 2 times in the last 7 days
- LATE_BURST_CLASS:
    >= 3 'late' statuses for a course in the same local day
"""

import os
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Any, Dict, Deque
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

# -------------------------------
# Env
# -------------------------------
load_dotenv()

def get_kafka_topic() -> str:
    topic = (
        os.getenv("KAFKA_TOPIC_CSV_SCHRODER")
        or os.getenv("SMOKER_TOPIC")  # fallback to example var
        or "attendance_csv"
    )
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_kafka_consumer_group_id() -> str:
    group_id = (
        os.getenv("KAFKA_GROUP_CSV_SCHRODER")
        or os.getenv("SMOKER_CONSUMER_GROUP_ID")  # fallback to example var
        or "attendance_group"
    )
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

# Tunables (env or defaults)
LOCAL_TZ = ZoneInfo("America/New_York")
ROLLING_WINDOW_SIZE = int(os.getenv("ATTENDANCE_ROLLING_WINDOW_SIZE", 10))
ATTENDANCE_RATE_THRESHOLD = float(os.getenv("ATTENDANCE_RATE_THRESHOLD", 0.85))
CHRONIC_WINDOW_DAYS = int(os.getenv("ATTENDANCE_CHRONIC_WINDOW_DAYS", 7))
CHRONIC_ABSENCES_THRESHOLD = int(os.getenv("ATTENDANCE_CHRONIC_ABSENCES_THRESHOLD", 2))
LATE_BURST_THRESHOLD = int(os.getenv("ATTENDANCE_LATE_BURST_THRESHOLD", 3))

# -------------------------------
# State
# -------------------------------
# course -> deque of last N statuses ("present"/"late"/"absent")
course_windows: Dict[str, Deque[str]] = defaultdict(lambda: deque(maxlen=ROLLING_WINDOW_SIZE))

# (course, local_date) -> counts
daily_course_counts: Dict[tuple[str, str], Dict[str, int]] = defaultdict(lambda: {"present": 0, "late": 0, "absent": 0, "total": 0})

# (course, local_date) -> late count (quick lookup for LATE_BURST)
late_per_course_day: Dict[tuple[str, str], int] = defaultdict(int)

# student -> list of absence datetimes (UTC→local converted) for sliding 7-day window
student_absences: Dict[str, Deque[datetime]] = defaultdict(deque)

# -------------------------------
# Helpers
# -------------------------------
def parse_ts_to_local(ts: str) -> datetime:
    """
    Parse ISO timestamp (supports trailing 'Z') to America/New_York aware datetime.
    """
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts).astimezone(LOCAL_TZ)

def attendance_rate_from_deque(dq: Deque[str]) -> float:
    if not dq:
        return 1.0
    attended = sum(1 for s in dq if s in ("present", "late"))
    return attended / len(dq)

def attendance_rate_daily(counts: Dict[str, int]) -> float:
    total = counts.get("total", 0)
    if total == 0:
        return 1.0
    attended = counts.get("present", 0) + counts.get("late", 0)
    return attended / total

def prune_old_absences(abs_q: Deque[datetime], now_local: datetime) -> None:
    """Keep only absences within CHRONIC_WINDOW_DAYS (local)."""
    cutoff = now_local - timedelta(days=CHRONIC_WINDOW_DAYS)
    while abs_q and abs_q[0] < cutoff:
        abs_q.popleft()

# -------------------------------
# Processing
# -------------------------------
def process_message(message: str) -> None:
    try:
        logger.debug(f"Raw message: {message}")
        rec: Dict[str, Any] = json.loads(message)

        ts = rec.get("ts")
        student = (rec.get("student") or "").strip()
        course = (rec.get("course") or "").strip()
        status = (rec.get("status") or "").strip().lower()

        if not ts or not course or not student or status not in ("present", "late", "absent"):
            logger.error(f"Invalid message format: {rec}")
            return

        local_dt = parse_ts_to_local(ts)
        local_date = local_dt.date().isoformat()
        hhmm = local_dt.strftime("%H:%M")

        # Log summary
        logger.info(f"[{local_date} {hhmm}] {course} | {student} → {status}")

        # --- Update rolling window per course ---
        cw = course_windows[course]
        cw.append(status)

        # --- Update daily per course counts ---
        key_cd = (course, local_date)
        daily_course_counts[key_cd]["total"] += 1
        daily_course_counts[key_cd][status] += 1

        # --- Window-based alert: ATTENDANCE_DROP_CLASS (last N events for course) ---
        if len(cw) == ROLLING_WINDOW_SIZE:
            rate = attendance_rate_from_deque(cw)
            if rate < ATTENDANCE_RATE_THRESHOLD:
                logger.warning(
                    f"ATTENDANCE_DROP_CLASS (window): course='{course}' "
                    f"rate={rate:.0%} over last {ROLLING_WINDOW_SIZE} events (< {ATTENDANCE_RATE_THRESHOLD:.0%})"
                )

        # --- Daily alert: ATTENDANCE_DROP_CLASS (today per course) ---
        daily_rate = attendance_rate_daily(daily_course_counts[key_cd])
        if daily_rate < ATTENDANCE_RATE_THRESHOLD:
            logger.warning(
                f"ATTENDANCE_DROP_CLASS (daily): course='{course}' date={local_date} "
                f"rate={daily_rate:.0%} (< {ATTENDANCE_RATE_THRESHOLD:.0%})"
            )

        # --- Late burst per course per day ---
        if status == "late":
            late_per_course_day[key_cd] += 1
            if late_per_course_day[key_cd] >= LATE_BURST_THRESHOLD:
                logger.warning(
                    f"LATE_BURST_CLASS: course='{course}' date={local_date} "
                    f"lates={late_per_course_day[key_cd]} (>= {LATE_BURST_THRESHOLD})"
                )

        # --- Chronic absences per student (7-day window by default) ---
        if status == "absent":
            abs_q = student_absences[student]
            abs_q.append(local_dt)
            prune_old_absences(abs_q, local_dt)
            if len(abs_q) >= CHRONIC_ABSENCES_THRESHOLD:
                logger.warning(
                    f"CHRONIC_ABSENCE_STUDENT: student='{student}' "
                    f"absences_last_{CHRONIC_WINDOW_DAYS}d={len(abs_q)} (>= {CHRONIC_ABSENCES_THRESHOLD})"
                )

    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error for message '{message}': {e}")
    except Exception as e:
        logger.error(f"Error processing message '{message}': {e}")

# -------------------------------
# Main
# -------------------------------
def main() -> None:
    logger.info("START attendance consumer (schroder).")

    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")
    logger.info(
        f"Config: WINDOW={ROLLING_WINDOW_SIZE}, RATE<{ATTENDANCE_RATE_THRESHOLD:.0%}, "
        f"CHRONIC_WINDOW_DAYS={CHRONIC_WINDOW_DAYS}, CHRONIC_ABSENCES>={CHRONIC_ABSENCES_THRESHOLD}, "
        f"LATE_BURST>={LATE_BURST_THRESHOLD}"
    )

    consumer = create_kafka_consumer(topic, group_id)

    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for msg in consumer:
            message_str: str = msg.value  # utils gives us str
            logger.debug(f"Received message at offset {msg.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")
        logger.info("END attendance consumer (schroder).")

if __name__ == "__main__":
    main()
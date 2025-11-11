import os
import json
import time
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

# --------------------------------------------------------------------
# Config: bridges and sensor types
# --------------------------------------------------------------------
BRIDGE_IDS = [1, 2, 3, 4, 5]
SENSOR_TYPES = ["temperature", "vibration", "tilt"]


def now_utc():
    """
    Helper to return a timezone-aware UTC datetime.
    This avoids the deprecated datetime.utcnow().
    """
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime) -> str:
    """
    Convert a UTC datetime to an ISO string with 'Z' suffix
    (e.g. 2025-11-08T10:26:35.123456Z).
    """
    return dt.isoformat().replace("+00:00", "Z")


def make_event(sensor_type: str, bridge_id: int):
    # 0–60s random lag to mimic late arrivals
    lag_seconds = random.randint(0, 60)
    event_time = now_utc() - timedelta(seconds=lag_seconds)

    # Sensor-specific value ranges
    if sensor_type == "temperature":
        value = random.uniform(5.0, 40.0)   # °C
    elif sensor_type == "vibration":
        value = random.uniform(0.0, 10.0)   # arbitrary units
    else:  # tilt
        value = random.uniform(0.0, 30.0)   # degrees

    return {
        "event_time": iso_utc(event_time),
        "bridge_id": bridge_id,
        "sensor_type": sensor_type,
        "value": float(round(value, 3)),
        "ingest_time": iso_utc(now_utc()),
    }


def write_batch(base_dir: Path, batch_events):
    """Write a batch of events to partitioned JSON files."""
    if not batch_events:
        return

    date_str = now_utc().strftime("%Y-%m-%d")

    # group by sensor type so we write into separate folders
    grouped = {"temperature": [], "vibration": [], "tilt": []}
    for e in batch_events:
        grouped[e["sensor_type"]].append(e)

    for sensor_type, events in grouped.items():
        if not events:
            continue

        if sensor_type == "temperature":
            out_dir = base_dir / "bridge_temperature" / f"date={date_str}"
        elif sensor_type == "vibration":
            out_dir = base_dir / "bridge_vibration" / f"date={date_str}"
        else:
            out_dir = base_dir / "bridge_tilt" / f"date={date_str}"

        out_dir.mkdir(parents=True, exist_ok=True)

        ts = now_utc().strftime("%Y%m%d_%H%M%S_%f")
        file_path = out_dir / f"events_{ts}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")

        print(f"Wrote {len(events)} events to {file_path}")


def run_generator(stream_dir: str, duration_seconds: int, rate_per_sec: int):
    """
    Core generator logic. You can call this directly or use
    start_bridge_generator() below for a nicer Colab API.
    """
    base_dir = Path(stream_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    batch = []
    flush_interval = 10  # seconds between file writes
    last_flush = time.time()

    print(
        f"Starting generator for {duration_seconds} seconds "
        f"at ~{rate_per_sec} events/sec"
    )

    while True:
        now = time.time()
        if duration_seconds > 0 and now - start_time >= duration_seconds:
            break

        # generate approx rate_per_sec events per second
        for _ in range(rate_per_sec):
            bridge_id = random.choice(BRIDGE_IDS)
            sensor_type = random.choice(SENSOR_TYPES)
            event = make_event(sensor_type, bridge_id)
            batch.append(event)

        # flush batch to disk every flush_interval seconds
        if now - last_flush >= flush_interval:
            write_batch(base_dir, batch)
            batch = []
            last_flush = now

        time.sleep(1)

    # final flush
    write_batch(base_dir, batch)
    print("Generator finished.")


# --------------------------------------------------------------------
# Colab-friendly wrapper
# --------------------------------------------------------------------
def start_bridge_generator(
    stream_dir="/content/streams",  # default path in Colab
    duration_seconds=60,
    rate_per_sec=10,
    test_seed=None,
):
    """
    Convenience function for running in notebooks / Colab.
    Just call start_bridge_generator(...) from a cell.
    """
    if test_seed is not None:
        random.seed(test_seed)

    run_generator(
        stream_dir=stream_dir,
        duration_seconds=duration_seconds,
        rate_per_sec=rate_per_sec,
    )

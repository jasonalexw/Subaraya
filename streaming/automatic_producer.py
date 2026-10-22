#!/usr/bin/env python3
"""Continuously generate Metropolia demo events and publish them to Kafka."""

import json
import os
import random
import signal
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "metropolia.events")
INTERVAL_SECONDS = float(os.getenv("AUTO_STREAM_INTERVAL_SECONDS", "5"))
ACCIDENT_EVERY_CYCLES = int(os.getenv("AUTO_ACCIDENT_EVERY_CYCLES", "13"))
RANDOM_SEED = int(os.getenv("AUTO_RANDOM_SEED", "2026"))
TIMEZONE = ZoneInfo("Asia/Jakarta")

ZONE_CAPACITY = {
    "Z01": 6000,
    "Z02": 5000,
    "Z03": 4000,
    "Z04": 3000,
}

running = True


def stop(_signum, _frame) -> None:
    global running
    running = False


def now_iso() -> str:
    return datetime.now(TIMEZONE).isoformat(timespec="seconds")


def base_event(event_type: str, zone_id: str, event_time: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": event_time,
        "zone_id": zone_id,
        "emitted_at": now_iso(),
    }


def build_traffic(zone_id: str, event_time: str, rng: random.Random) -> dict:
    capacity = ZONE_CAPACITY[zone_id]
    rainfall = round(rng.choice([0.0, 0.0, 0.0, rng.uniform(0.2, 8.0)]), 2)
    payload = base_event("traffic", zone_id, event_time)
    payload.update(
        {
            "traffic_volume": rng.randint(int(capacity * 0.35), int(capacity * 0.95)),
            "temperature_c": round(rng.uniform(26.0, 34.0), 1),
            "rainfall": rainfall,
            "weather_main": "Rain" if rainfall > 0 else "Clear",
        }
    )
    return payload


def build_air_quality(zone_id: str, event_time: str, rng: random.Random) -> dict:
    payload = base_event("air_quality", zone_id, event_time)
    payload.update(
        {
            "pm25": round(rng.uniform(15.0, 110.0), 1),
            "pm10": round(rng.uniform(25.0, 160.0), 1),
            "no2": round(rng.uniform(10.0, 65.0), 1),
            "co": round(rng.uniform(250.0, 1200.0), 1),
            "o3": round(rng.uniform(10.0, 80.0), 1),
            "temperature_c": round(rng.uniform(26.0, 34.0), 1),
            "rainfall": 0.0,
            "wind_speed": round(rng.uniform(0.5, 6.0), 1),
        }
    )
    return payload


def build_accident(zone_id: str, event_time: str, rng: random.Random) -> dict:
    severity = rng.choices(
        ["SLIGHT", "SERIOUS", "FATAL"], weights=[75, 22, 3], k=1
    )[0]
    payload = base_event("accident", zone_id, event_time)
    payload.update(
        {
            "severity": severity,
            "casualty_count": rng.randint(0, 3),
            "vehicle_count": rng.randint(1, 4),
        }
    )
    return payload


def connect_producer(max_attempts: int = 30) -> KafkaProducer:
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
                key_serializer=lambda value: value.encode("utf-8"),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks="all",
                retries=5,
            )
        except NoBrokersAvailable as error:
            last_error = error
            print(f"Kafka belum siap (percobaan {attempt}/{max_attempts}).", flush=True)
            time.sleep(2)
    raise RuntimeError(f"Tidak dapat terhubung ke Kafka: {last_error}")


def publish(producer: KafkaProducer, payload: dict) -> None:
    metadata = producer.send(
        TOPIC, key=payload["zone_id"], value=payload
    ).get(timeout=20)
    print(
        json.dumps(
            {
                "event_type": payload["event_type"],
                "zone_id": payload["zone_id"],
                "event_time": payload["event_time"],
                "event_id": payload["event_id"],
                "partition": metadata.partition,
                "offset": metadata.offset,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


def main() -> None:
    if INTERVAL_SECONDS <= 0:
        raise ValueError("AUTO_STREAM_INTERVAL_SECONDS harus lebih besar dari 0")
    if ACCIDENT_EVERY_CYCLES <= 0:
        raise ValueError("AUTO_ACCIDENT_EVERY_CYCLES harus lebih besar dari 0")

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    rng = random.Random(RANDOM_SEED)
    producer = connect_producer()
    zones = tuple(ZONE_CAPACITY)
    cycle = 0
    print(
        f"Automatic producer aktif: interval={INTERVAL_SECONDS}s, "
        f"accident_every={ACCIDENT_EVERY_CYCLES} cycle(s)",
        flush=True,
    )

    try:
        while running:
            cycle += 1
            zone_id = zones[(cycle - 1) % len(zones)]
            event_time = now_iso()
            events = [
                build_traffic(zone_id, event_time, rng),
                build_air_quality(zone_id, event_time, rng),
            ]
            if (cycle - 1) % ACCIDENT_EVERY_CYCLES == 0:
                events.append(build_accident(zone_id, event_time, rng))

            for payload in events:
                publish(producer, payload)
            producer.flush()

            deadline = time.monotonic() + INTERVAL_SECONDS
            while running and time.monotonic() < deadline:
                time.sleep(min(0.5, max(0.0, deadline - time.monotonic())))
    finally:
        producer.flush()
        producer.close()
        print("Automatic producer dihentikan.", flush=True)


if __name__ == "__main__":
    main()

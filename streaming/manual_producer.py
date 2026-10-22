#!/usr/bin/env python3
"""Send one manually entered Metropolia sensor event to Kafka."""

import argparse
import json
import os
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


VALID_ZONES = ("Z01", "Z02", "Z03", "Z04")
TOPIC = os.getenv("KAFKA_TOPIC", "metropolia.events")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")


def current_event_time() -> str:
    return datetime.now(ZoneInfo("Asia/Jakarta")).isoformat(timespec="seconds")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Kirim satu event manual ke Kafka untuk demo streaming Metropolia."
    )
    parser.add_argument(
        "--event-time",
        default=None,
        help="Waktu ISO-8601. Default: waktu Asia/Jakarta saat perintah dijalankan.",
    )
    subparsers = parser.add_subparsers(dest="event_type", required=True)

    traffic = subparsers.add_parser("traffic", help="Kirim observasi traffic.")
    traffic.add_argument("--zone", required=True, choices=VALID_ZONES)
    traffic.add_argument("--volume", required=True, type=int)
    traffic.add_argument("--temperature", type=float, default=30.0)
    traffic.add_argument("--rain", type=float, default=0.0)
    traffic.add_argument("--weather", default="Clear")

    air = subparsers.add_parser("air-quality", help="Kirim observasi kualitas udara.")
    air.add_argument("--zone", required=True, choices=VALID_ZONES)
    air.add_argument("--pm25", required=True, type=float)
    air.add_argument("--pm10", type=float)
    air.add_argument("--no2", type=float)
    air.add_argument("--co", type=float)
    air.add_argument("--o3", type=float)
    air.add_argument("--temperature", type=float, default=30.0)
    air.add_argument("--rain", type=float, default=0.0)
    air.add_argument("--wind-speed", type=float, default=0.0)

    accident = subparsers.add_parser("accident", help="Kirim satu kejadian kecelakaan.")
    accident.add_argument("--zone", required=True, choices=VALID_ZONES)
    accident.add_argument(
        "--severity",
        required=True,
        choices=("fatal", "serious", "slight"),
    )
    accident.add_argument("--casualties", type=int, default=0)
    accident.add_argument("--vehicles", type=int, default=1)

    return parser


def build_payload(args: argparse.Namespace) -> dict:
    payload = {
        "event_id": str(uuid.uuid4()),
        "event_type": args.event_type.replace("-", "_"),
        "event_time": args.event_time or current_event_time(),
        "zone_id": args.zone,
        "emitted_at": current_event_time(),
    }

    if args.event_type == "traffic":
        if args.volume < 0:
            raise ValueError("--volume tidak boleh negatif")
        payload.update(
            {
                "traffic_volume": args.volume,
                "temperature_c": args.temperature,
                "rainfall": args.rain,
                "weather_main": args.weather,
            }
        )
    elif args.event_type == "air-quality":
        if args.pm25 < 0:
            raise ValueError("--pm25 tidak boleh negatif")
        payload.update(
            {
                "pm25": args.pm25,
                "pm10": args.pm10,
                "no2": args.no2,
                "co": args.co,
                "o3": args.o3,
                "temperature_c": args.temperature,
                "rainfall": args.rain,
                "wind_speed": args.wind_speed,
            }
        )
    else:
        if args.casualties < 0 or args.vehicles < 0:
            raise ValueError("--casualties dan --vehicles tidak boleh negatif")
        payload.update(
            {
                "severity": args.severity.upper(),
                "casualty_count": args.casualties,
                "vehicle_count": args.vehicles,
            }
        )

    return payload


def connect_producer(max_attempts: int = 20) -> KafkaProducer:
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
            print(f"Kafka belum siap (percobaan {attempt}/{max_attempts}).")
            time.sleep(2)
    raise RuntimeError(f"Tidak dapat terhubung ke Kafka: {last_error}")


def main() -> None:
    args = build_parser().parse_args()
    payload = build_payload(args)
    producer = connect_producer()
    try:
        metadata = producer.send(TOPIC, key=payload["zone_id"], value=payload).get(
            timeout=20
        )
        producer.flush()
    finally:
        producer.close()

    print("Event berhasil dikirim ke Kafka")
    print(f"Topic     : {metadata.topic}")
    print(f"Partition : {metadata.partition}")
    print(f"Offset    : {metadata.offset}")
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()

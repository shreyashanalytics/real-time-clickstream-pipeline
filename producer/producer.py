"""Synthetic clickstream producer: pushes JSON events to Apache Kafka.

Event shape::

    {"event_id": "...", "user_id": 42, "event_type": "purchase",
     "event_timestamp": "2026-01-01T10:00:00.123Z", "product_id": 1007,
     "category": "fashion", "price": 49.99}

Teaching features (all adjustable from the command line):
  * ``--interval`` / ``--jitter``  custom, slightly random gap between events
  * ``--late-rate``                some events carry an OLD timestamp (late data)
  * ``--duplicate-rate``           optionally re-send events (default 0)

Examples::

    python producer.py --interval 0.1                 # ~10 events / second
    python producer.py --interval 1 --duration 60     # 1 event / second for a minute
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import signal
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

logger = logging.getLogger("producer")

EVENT_TYPES = ("page_view", "add_to_cart", "purchase")
EVENT_WEIGHTS = (0.70, 0.20, 0.10)
CATEGORIES = ("electronics", "fashion", "home", "beauty", "sports", "books")


@dataclass(frozen=True)
class Product:
    """A catalog item; every event for it carries the same price."""

    product_id: int
    category: str
    price: float


def build_catalog(rng: random.Random, size: int = 60) -> list[Product]:
    """Create a small, reproducible product catalog."""
    return [
        Product(1000 + i, rng.choice(CATEGORIES), round(rng.uniform(5, 500), 2))
        for i in range(size)
    ]


def format_timestamp(value: datetime) -> str:
    """ISO-8601 in UTC with millisecond precision, e.g. 2026-01-01T10:00:00.123Z."""
    value = value.astimezone(UTC)
    return value.strftime("%Y-%m-%dT%H:%M:%S.") + f"{value.microsecond // 1000:03d}Z"


def make_event(
    rng: random.Random,
    catalog: list[Product],
    now: datetime,
    *,
    n_users: int = 500,
    late_rate: float = 0.05,
    max_late_seconds: float = 240.0,
) -> dict[str, Any]:
    """Build one synthetic event. Late events get a timestamp from the past."""
    product = rng.choice(catalog)
    event_time = now
    if rng.random() < late_rate:
        event_time = now - timedelta(seconds=rng.uniform(30, max_late_seconds))
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "user_id": rng.randint(1, n_users),
        "event_type": rng.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0],
        "event_timestamp": format_timestamp(event_time),
        "product_id": product.product_id,
        "category": product.category,
        "price": product.price,
    }


# --------------------------------------------------------------------------- #
# Kafka plumbing
# --------------------------------------------------------------------------- #
def wait_for_broker(bootstrap: str, timeout_s: float = 120.0) -> None:
    """Block until the broker answers, so we survive slow container start-up."""
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            admin.list_topics(timeout=5)
            logger.info("Connected to Kafka at %s", bootstrap)
            return
        except KafkaException as exc:
            if time.monotonic() > deadline:
                raise RuntimeError(f"Kafka not reachable at {bootstrap}") from exc
            logger.warning("Kafka not ready yet (%s); retrying...", exc)
            time.sleep(3)


def build_producer(bootstrap: str):
    """Idempotent, acks=all producer: retries cannot create duplicates."""
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "clickstream-producer",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "delivery.timeout.ms": 30_000,
        }
    )


class Stats:
    """Counters updated from delivery callbacks."""

    def __init__(self) -> None:
        self.sent = 0
        self.delivered = 0
        self.failed = 0

    def on_delivery(self, err, msg) -> None:  # noqa: ARG002 - confluent callback signature
        if err is not None:
            self.failed += 1
            logger.error("Delivery failed: %s", err)
        else:
            self.delivered += 1


def send(producer, topic: str, event: dict[str, Any], stats: Stats) -> None:
    """Serialize and enqueue one event, backing off if the local queue is full."""
    from confluent_kafka import KafkaException

    payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
    key = str(event["user_id"]).encode("utf-8")  # same user -> same partition
    while True:
        try:
            producer.produce(topic, key=key, value=payload, on_delivery=stats.on_delivery)
            stats.sent += 1
            break
        except BufferError:
            producer.poll(0.5)  # queue full: let callbacks drain, then retry
        except KafkaException:
            stats.failed += 1
            logger.exception("Could not enqueue event")
            break
    producer.poll(0)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic clickstream producer for Kafka")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "clickstream.events"))
    parser.add_argument("--interval", type=float, default=0.2, help="mean seconds between events")
    parser.add_argument("--jitter", type=float, default=0.5, help="random +/- share of interval")
    parser.add_argument("--duration", type=float, default=0, help="stop after N seconds (0=never)")
    parser.add_argument("--users", type=int, default=500)
    parser.add_argument("--late-rate", type=float, default=0.05, help="share of late events")
    parser.add_argument("--max-late-seconds", type=float, default=240.0)
    parser.add_argument("--duplicate-rate", type=float, default=0.0)
    parser.add_argument("--seed", type=int, default=None, help="make the data reproducible")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    if args.interval < 0:
        parser.error("--interval must be >= 0")
    for name in ("jitter", "late_rate", "duplicate_rate"):
        if not 0 <= getattr(args, name) <= 1:
            parser.error(f"--{name.replace('_', '-')} must be between 0 and 1")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    rng = random.Random(args.seed)
    catalog = build_catalog(rng)
    stats = Stats()

    try:
        wait_for_broker(args.bootstrap)
        producer = build_producer(args.bootstrap)
    except Exception:
        logger.exception("Cannot start producer")
        return 1

    deadline = time.monotonic() + args.duration if args.duration else None
    last_report = time.monotonic()
    logger.info("Producing to '%s' every ~%.3fs (Ctrl+C to stop)", args.topic, args.interval)

    try:
        while not stop.is_set():
            if deadline and time.monotonic() >= deadline:
                break
            event = make_event(
                rng,
                catalog,
                datetime.now(UTC),
                n_users=args.users,
                late_rate=args.late_rate,
                max_late_seconds=args.max_late_seconds,
            )
            send(producer, args.topic, event, stats)
            if rng.random() < args.duplicate_rate:
                send(producer, args.topic, event, stats)

            if time.monotonic() - last_report >= 10:
                logger.info(
                    "sent=%d delivered=%d failed=%d", stats.sent, stats.delivered, stats.failed
                )
                last_report = time.monotonic()

            gap = args.interval * (1 + rng.uniform(-args.jitter, args.jitter))
            stop.wait(max(gap, 0))
    finally:
        remaining = producer.flush(15)
        logger.info(
            "Stopped. sent=%d delivered=%d failed=%d unflushed=%d",
            stats.sent,
            stats.delivered,
            stats.failed,
            remaining,
        )
    return 0 if stats.failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())

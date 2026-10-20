"""Unit tests for the event generator (no Kafka needed)."""

import random
from datetime import UTC, datetime

from producer import EVENT_TYPES, build_catalog, format_timestamp, make_event

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def _events(n=200, seed=7, **kwargs):
    rng = random.Random(seed)
    catalog = build_catalog(rng)
    return [make_event(rng, catalog, NOW, **kwargs) for _ in range(n)]


def test_same_seed_gives_same_events():
    assert _events() == _events()


def test_event_has_expected_fields_and_values():
    for event in _events():
        assert set(event) == {
            "event_id", "user_id", "event_type", "event_timestamp",
            "product_id", "category", "price",
        }  # fmt: skip
        assert event["event_type"] in EVENT_TYPES
        assert event["price"] > 0


def test_event_ids_are_unique():
    ids = [e["event_id"] for e in _events(500)]
    assert len(ids) == len(set(ids))


def test_on_time_events_use_current_time():
    for event in _events(50, late_rate=0.0):
        assert event["event_timestamp"] == format_timestamp(NOW)


def test_late_events_have_older_timestamps():
    for event in _events(50, late_rate=1.0, max_late_seconds=240):
        assert event["event_timestamp"] < format_timestamp(NOW)


def test_timestamp_format():
    assert format_timestamp(NOW) == "2026-01-01T12:00:00.000Z"

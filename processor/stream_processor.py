"""PySpark Structured Streaming: Kafka -> windowed aggregates -> ClickHouse.

Three streaming queries read the same Kafka topic:

  Q1  events_raw        every valid event (append)
  Q2  sales_tumbling    revenue / orders per category in TUMBLING windows (default 5 min)
  Q3  activity_sliding  events / users per type in SLIDING windows (default 10 min, slide 1 min)

Late data: ``withWatermark("event_ts", WATERMARK)`` tells Spark to keep a window open
until event time has moved WATERMARK past its end. Events older than that are dropped
from the aggregates (but are still kept in events_raw, so you can audit the difference).

Sink: ``foreachBatch`` + the ClickHouse HTTP client. Spark runs in *update* mode, so a
window is re-emitted whenever it changes; ReplacingMergeTree keeps the newest version.
Failures are retried with back-off; if retries run out the query stops, Docker restarts
the container, and Spark resumes from its checkpoint (no data loss).
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import clickhouse_connect
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger("stream_processor")

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
VALID_EVENT_TYPES = ["page_view", "add_to_cart", "purchase"]

EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("user_id", T.LongType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_timestamp", T.StringType()),
        T.StructField("product_id", T.LongType()),
        T.StructField("category", T.StringType()),
        T.StructField("price", T.DoubleType()),
    ]
)


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Config:
    """All settings come from environment variables (see docker-compose.yml)."""

    kafka_bootstrap: str
    topic: str
    starting_offsets: str
    max_offsets_per_trigger: int
    ch_host: str
    ch_port: int
    ch_user: str
    ch_password: str
    ch_database: str
    checkpoint_dir: str
    trigger_interval: str
    watermark: str
    sales_window: str
    activity_window: str
    activity_slide: str
    driver_memory: str

    @classmethod
    def from_env(cls) -> Config:
        env = os.environ.get
        return cls(
            kafka_bootstrap=env("KAFKA_BOOTSTRAP", "kafka:29092"),
            topic=env("KAFKA_TOPIC", "clickstream.events"),
            starting_offsets=env("STARTING_OFFSETS", "earliest"),
            max_offsets_per_trigger=int(env("MAX_OFFSETS_PER_TRIGGER", "5000")),
            ch_host=env("CLICKHOUSE_HOST", "clickhouse"),
            ch_port=int(env("CLICKHOUSE_PORT", "8123")),
            ch_user=env("CLICKHOUSE_USER", "app"),
            ch_password=env("CLICKHOUSE_PASSWORD", "app_pw"),
            ch_database=env("CLICKHOUSE_DB", "clickstream"),
            checkpoint_dir=env("CHECKPOINT_DIR", "/checkpoints"),
            trigger_interval=env("TRIGGER_INTERVAL", "10 seconds"),
            watermark=env("WATERMARK", "2 minutes"),
            sales_window=env("SALES_WINDOW", "5 minutes"),
            activity_window=env("ACTIVITY_WINDOW", "10 minutes"),
            activity_slide=env("ACTIVITY_SLIDE", "1 minute"),
            driver_memory=env("SPARK_DRIVER_MEMORY", "1g"),
        )


# --------------------------------------------------------------------------- #
# Spark: source and transformations
# --------------------------------------------------------------------------- #
def build_spark(cfg: Config) -> SparkSession:
    """Local-mode Spark session (one container = one 'cluster', fine for a laptop)."""
    spark = (
        SparkSession.builder.appName("clickstream-stream-processor")
        .master("local[4]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.jars.ivy", "/opt/ivy")
        .config("spark.driver.memory", cfg.driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_events(spark: SparkSession, cfg: Config) -> DataFrame:
    """Kafka -> parsed, validated events with a proper ``event_ts`` timestamp column."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap)
        .option("subscribe", cfg.topic)
        .option("startingOffsets", cfg.starting_offsets)
        .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
    )
    # Malformed JSON parses to all-nulls and is silently filtered out here.
    return parsed.filter(
        F.col("event_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("event_ts").isNotNull()
        & F.col("event_type").isin(VALID_EVENT_TYPES)
        & (F.col("price") >= 0)
    )


def raw_events_stream(events: DataFrame) -> DataFrame:
    return events.select(
        "event_id", "user_id", "event_type", "event_ts", "product_id", "category", "price"
    )


def sales_tumbling_stream(events: DataFrame, cfg: Config) -> DataFrame:
    """Revenue per category in non-overlapping windows (e.g. 10:00-10:05, 10:05-10:10)."""
    return (
        events.filter(F.col("event_type") == "purchase")
        .withWatermark("event_ts", cfg.watermark)
        .groupBy(F.window("event_ts", cfg.sales_window), "category")
        .agg(
            F.sum("price").alias("revenue"),
            F.count(F.lit(1)).alias("orders"),
            F.approx_count_distinct("user_id").alias("buyers"),
        )
    )


def activity_sliding_stream(events: DataFrame, cfg: Config) -> DataFrame:
    """Events per type in overlapping windows (e.g. last 10 min, moving every minute)."""
    return (
        events.withWatermark("event_ts", cfg.watermark)
        .groupBy(F.window("event_ts", cfg.activity_window, cfg.activity_slide), "event_type")
        .agg(
            F.count(F.lit(1)).alias("events"),
            F.approx_count_distinct("user_id").alias("users"),
        )
    )


# --------------------------------------------------------------------------- #
# ClickHouse sink
# --------------------------------------------------------------------------- #
def _utc(value: datetime) -> datetime:
    """Spark hands back naive datetimes (container TZ=UTC); make them explicit."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _money(value: Any) -> Decimal:
    return Decimal(str(round(float(value or 0), 2)))


def event_row(row: Row, _now: datetime) -> tuple:
    return (
        row.event_id,
        int(row.user_id),
        row.event_type,
        _utc(row.event_ts),
        int(row.product_id),
        row.category or "unknown",
        _money(row.price),
    )


def sales_row(row: Row, now: datetime) -> tuple:
    return (
        _utc(row.window.start),
        _utc(row.window.end),
        row.category or "unknown",
        _money(row.revenue),
        int(row.orders),
        int(row.buyers),
        now,
    )


def activity_row(row: Row, now: datetime) -> tuple:
    return (
        _utc(row.window.start),
        _utc(row.window.end),
        row.event_type,
        int(row.events),
        int(row.users),
        now,
    )


class ClickHouseWriter:
    """foreachBatch target. One instance (and HTTP client) per streaming query."""

    def __init__(
        self,
        cfg: Config,
        table: str,
        columns: Sequence[str],
        to_row: Callable[[Row, datetime], tuple],
        max_attempts: int = 5,
        chunk_size: int = 5000,
    ) -> None:
        self.cfg = cfg
        self.table = table
        self.columns = list(columns)
        self.to_row = to_row
        self.max_attempts = max_attempts
        self.chunk_size = chunk_size
        self._client = None

    def _connect(self):
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.cfg.ch_host,
                port=self.cfg.ch_port,
                username=self.cfg.ch_user,
                password=self.cfg.ch_password,
                database=self.cfg.ch_database,
            )
        return self._client

    def write_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        now = datetime.now(UTC)
        # Batches are small (a few thousand rows at most), so collecting is fine here.
        rows = [self.to_row(r, now) for r in batch_df.collect()]
        if not rows:
            return
        for start in range(0, len(rows), self.chunk_size):
            self._insert(rows[start : start + self.chunk_size])
        logger.info("batch %d -> %s: %d rows", batch_id, self.table, len(rows))

    def _insert(self, rows: list[tuple]) -> None:
        for attempt in range(1, self.max_attempts + 1):
            try:
                self._connect().insert(self.table, rows, column_names=self.columns)
                return
            except Exception as exc:  # noqa: BLE001 - any client/network error is retried
                self._client = None  # force a fresh connection next time
                if attempt == self.max_attempts:
                    logger.error("Giving up on %s after %d attempts", self.table, attempt)
                    raise
                delay = min(2**attempt, 30)
                logger.warning(
                    "Insert into %s failed (%s); retry %d/%d in %ds",
                    self.table, exc, attempt, self.max_attempts, delay,
                )  # fmt: skip
                time.sleep(delay)


# --------------------------------------------------------------------------- #
# Wiring
# --------------------------------------------------------------------------- #
def start_query(name: str, df: DataFrame, mode: str, cfg: Config, writer: ClickHouseWriter):
    return (
        df.writeStream.queryName(name)
        .outputMode(mode)
        .trigger(processingTime=cfg.trigger_interval)
        .option("checkpointLocation", f"{cfg.checkpoint_dir}/{name}")
        .foreachBatch(writer.write_batch)
        .start()
    )


def start_all(spark: SparkSession, cfg: Config) -> None:
    events = read_events(spark, cfg)

    start_query(
        "events_raw",
        raw_events_stream(events),
        "append",
        cfg,
        ClickHouseWriter(
            cfg,
            "events_raw",
            ["event_id", "user_id", "event_type", "event_ts", "product_id", "category", "price"],
            event_row,
        ),
    )
    start_query(
        "sales_tumbling",
        sales_tumbling_stream(events, cfg),
        "update",
        cfg,
        ClickHouseWriter(
            cfg,
            "sales_tumbling",
            ["window_start", "window_end", "category", "revenue", "orders", "buyers", "updated_at"],
            sales_row,
        ),
    )
    start_query(
        "activity_sliding",
        activity_sliding_stream(events, cfg),
        "update",
        cfg,
        ClickHouseWriter(
            cfg,
            "activity_sliding",
            ["window_start", "window_end", "event_type", "events", "users", "updated_at"],
            activity_row,
        ),
    )


def log_progress(spark: SparkSession) -> None:
    for query in spark.streams.active:
        progress = query.lastProgress
        if progress:
            logger.info(
                "[%s] rows=%s watermark=%s",
                query.name,
                progress.get("numInputRows"),
                progress.get("eventTime", {}).get("watermark", "-"),
            )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = Config.from_env()
    logger.info(
        "Starting: topic=%s watermark=%s sales_window=%s activity=%s/%s",
        cfg.topic, cfg.watermark, cfg.sales_window, cfg.activity_window, cfg.activity_slide,
    )  # fmt: skip

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    spark = build_spark(cfg)
    exit_code = 0
    try:
        start_all(spark, cfg)
        last_report = time.monotonic()
        while not stop.is_set():
            # Raises StreamingQueryException if any query died -> handled below.
            spark.streams.awaitAnyTermination(timeout=5)
            if time.monotonic() - last_report >= 30:
                log_progress(spark)
                last_report = time.monotonic()
    except Exception:
        logger.exception("Streaming job failed")
        exit_code = 1
    finally:
        logger.info("Shutting down streaming queries...")
        for query in spark.streams.active:
            try:
                query.stop()
            except Exception:  # noqa: BLE001
                logger.exception("Could not stop query %s", query.name)
        spark.stop()
    return exit_code


if __name__ == "__main__":
    sys.exit(main())

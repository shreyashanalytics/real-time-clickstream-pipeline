-- ClickHouse schema for the clickstream pipeline.
-- Runs automatically the first time the ClickHouse container starts.
--
-- Design note: Spark runs in "update" output mode, so the SAME window is
-- re-emitted every time new events change it. ReplacingMergeTree(updated_at)
-- keeps only the newest version per sorting key. (SummingMergeTree would ADD
-- the repeated rows together and double-count - wrong for this use case.)
-- Always read these tables with FINAL (or through the v_* views below).

CREATE DATABASE IF NOT EXISTS clickstream;

-- 1) Every valid event, as received. Duplicate event_ids collapse on merge.
CREATE TABLE IF NOT EXISTS clickstream.events_raw
(
    event_id    String,
    user_id     UInt32,
    event_type  LowCardinality(String),
    event_ts    DateTime64(3, 'UTC'),
    product_id  UInt32,
    category    LowCardinality(String),
    price       Decimal(10, 2),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (event_type, toStartOfMinute(event_ts), event_id)
TTL toDateTime(event_ts) + INTERVAL 7 DAY;

-- 2) Tumbling-window sales: revenue and orders per category per window.
CREATE TABLE IF NOT EXISTS clickstream.sales_tumbling
(
    window_start DateTime('UTC'),
    window_end   DateTime('UTC'),
    category     LowCardinality(String),
    revenue      Decimal(18, 2),
    orders       UInt64,
    buyers       UInt64,
    updated_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, category)
TTL window_start + INTERVAL 30 DAY;

-- 3) Sliding-window activity: events and unique users per event type.
CREATE TABLE IF NOT EXISTS clickstream.activity_sliding
(
    window_start DateTime('UTC'),
    window_end   DateTime('UTC'),
    event_type   LowCardinality(String),
    events       UInt64,
    users        UInt64,
    updated_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, event_type)
TTL window_start + INTERVAL 30 DAY;

-- ---------------------------------------------------------------------------
-- Views for dashboards (FINAL = "give me only the newest version of each row")
-- ---------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS clickstream.v_sales_tumbling AS
SELECT window_start, window_end, category, revenue, orders, buyers
FROM clickstream.sales_tumbling FINAL;

CREATE VIEW IF NOT EXISTS clickstream.v_activity_sliding AS
SELECT window_start, window_end, event_type, events, users
FROM clickstream.activity_sliding FINAL;

CREATE VIEW IF NOT EXISTS clickstream.v_funnel_30min AS
SELECT event_type, count() AS events, uniqExact(user_id) AS users
FROM clickstream.events_raw FINAL
WHERE event_ts >= now() - INTERVAL 30 MINUTE
GROUP BY event_type;

-- Late-data audit: purchases that reached Kafka but missed their window
-- because they arrived after the watermark had already closed it.
CREATE VIEW IF NOT EXISTS clickstream.v_late_data_audit AS
SELECT
    (SELECT count() FROM clickstream.events_raw FINAL WHERE event_type = 'purchase') AS purchases_received,
    (SELECT sum(orders) FROM clickstream.sales_tumbling FINAL) AS purchases_in_windows,
    purchases_received - purchases_in_windows AS dropped_by_watermark;

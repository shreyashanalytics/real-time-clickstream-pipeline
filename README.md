# Real-Time Clickstream Analytics Pipeline

Synthetic shopper events stream through **Apache Kafka**, are aggregated in real time by
**PySpark Structured Streaming** (tumbling + sliding windows, watermarking for late data),
stored in **ClickHouse**, and shown on a live **Apache Superset** dashboard.
100% free, open source, runs locally with one `docker compose up`.

## Architecture

```
+---------------------+
| producer.py         |  synthetic events: page_view / add_to_cart / purchase
| (confluent-kafka)   |  ~5% arrive LATE on purpose
+----------+----------+
           | JSON, key = user_id
           v
+==================================+
| Apache Kafka (KRaft, no ZK)      |
| topic: clickstream.events (3 p.) |
+==================+===============+
                   |
                   v
+------------------------------------------------------------+
| PySpark Structured Streaming  (stream_processor.py)        |
|  parse JSON -> validate -> withWatermark(event_ts, 2 min)  |
|                                                            |
|  Q1 raw events (append)                                    |
|  Q2 TUMBLING 5-min window: revenue/orders per category     |
|  Q3 SLIDING 10-min/1-min window: events per type           |
|     foreachBatch + retries + checkpoints (restart-safe)    |
+------------------------------+-----------------------------+
                               | HTTP insert (clickhouse-connect)
                               v
+------------------------------------------------------------+
| ClickHouse                                                 |
|  events_raw        ReplacingMergeTree (dedupe by event_id) |
|  sales_tumbling    ReplacingMergeTree(updated_at)          |
|  activity_sliding  ReplacingMergeTree(updated_at)          |
|  views: v_sales_tumbling, v_activity_sliding,              |
|         v_funnel_30min, v_late_data_audit                  |
+------------------------------+-----------------------------+
                               | SQL (clickhousedb://)
                               v
+------------------------------------------------------------+
| Apache Superset  http://localhost:8088  (auto-refresh 10s) |
+------------------------------------------------------------+
```

## Screenshots

### Live dashboard (Superset)
![Dashboard](docs/images/superset-dashboard.png)

### Services running (Docker)
![Docker services](docs/images/docker-services.png)

### Late-data handling (watermark)
![Late data audit](docs/images/late-data-audit.png)

### Streaming queries (Spark UI)
![Spark UI](docs/images/spark-ui.png)


## Quick start

```bash
cp .env.example .env            # Windows: copy .env.example .env
docker compose up --build -d    # first build ~5-10 min
docker compose ps
docker compose logs -f stream-processor
```

| Service | URL / port | Login |
|---|---|---|
| Superset | http://localhost:8088 | admin / admin |
| Spark UI | http://localhost:4040 | - |
| ClickHouse HTTP | http://localhost:8123 | app / app_pw |
| Kafka (from your PC) | localhost:9092 | - |

## Verify end to end

```bash
make topic      # 5 raw events from Kafka
make sql        # ClickHouse prompt, then try:
```
```sql
SELECT count() FROM events_raw;
SELECT * FROM v_sales_tumbling ORDER BY window_start DESC, revenue DESC LIMIT 10;
SELECT * FROM v_activity_sliding ORDER BY window_start DESC LIMIT 10;
SELECT * FROM v_late_data_audit;
```

## Superset dashboard (5 minutes)

1. Settings -> Database Connections -> + Database -> **ClickHouse Connect**
   (or "Other" with SQLAlchemy URI `clickhousedb://app:app_pw@clickhouse:8123/clickstream`).
2. Datasets -> + Dataset -> schema `clickstream` -> `v_sales_tumbling`; repeat for `v_activity_sliding`, `v_funnel_30min`.
3. Charts: revenue by window (time-series, x = `window_start`, metric `SUM(revenue)`, dimension `category`);
   funnel (`v_funnel_30min`, dimension `event_type`, metric `SUM(events)`); big number `SUM(revenue)`.
4. Dashboard -> add charts -> `...` -> **Set auto-refresh interval -> 10 seconds**.


## Design decisions

- **Kafka in KRaft mode:** no Zookeeper to run; fewer moving parts.
- **Event time, not arrival time:** windows use `event_timestamp`; the watermark bounds state and defines "too late".
- **Update output mode + ReplacingMergeTree:** each window is re-emitted as it grows; ClickHouse keeps the newest version. SummingMergeTree would double-count.
- **foreachBatch + retries/back-off:** simple, dependency-free sink; replays are safe because rows are idempotent.
- **Idempotent producer** (`acks=all`, `enable.idempotence`): retries never create duplicates.
- **Invalid events** (bad JSON, unknown type, negative price) are filtered before aggregation.

## Troubleshooting

| Symptom | Fix |
|---|---|
| Containers keep restarting / out of memory | Give Docker 8 GB (Windows: `.wslconfig`), or lower `SPARK_DRIVER_MEMORY` |
| Superset page not loading | Wait 1-2 min; check `docker compose logs superset` |
| Tables empty | `docker compose logs stream-processor`; wait ~30 s after start |
| Tables missing in ClickHouse | They are created on the *first* start only; `make reset` then `make up` |
| `port is already allocated` | Another program uses 8088/8123/9092/4040; stop it or change the left-hand port in `docker-compose.yml` |


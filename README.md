# StreamMart — Real-Time E-Commerce Analytics Pipeline

A production-grade streaming data pipeline that ingests clickstream events from a simulated e-commerce platform, processes them with Spark Structured Streaming, stores raw data in a MinIO data lake, and surfaces real-time KPIs in Grafana — all orchestrated by Apache Airflow.

---

## Architecture

```
Event Simulator
     │  (5 event types @ configurable rate)
     ▼
Kafka (KRaft, 6 topics, 3 partitions each)
     │
     ├──► Spark: Raw Event Writer ──────────────────► MinIO (Parquet data lake)
     │         partitioned by year/month/day/event_type
     │
     ├──► Spark: Window Aggregator ────────────────► PostgreSQL metrics_1min
     │         1-min tumbling windows, ON CONFLICT upsert
     │
     ├──► Spark: Session Tracker ──────────────────► PostgreSQL session_summary
     │         session_window (30-min gap), ON CONFLICT upsert
     │
     └──► Spark: Revenue Aggregator ───────────────► PostgreSQL product_performance
               hourly revenue + product KPIs

PostgreSQL
     │
     ├──► postgres-rollup (every 60s): metrics_1min → metrics_5min,
     │         product_performance upsert, DQ checks, retention deletes
     │
     └──► Airflow DAGs (scheduled batch layer):
               • daily_batch_processing  @ 02:00 → daily_summary, product_daily_performance
               • daily_summary           @ 01:00 → daily_revenue
               • data_quality            every 6h → data_quality_checks
               • pipeline_health_check  every 15m → pipeline_monitoring

Grafana ◄──── PostgreSQL (real-time dashboards)
Prometheus / Loki ◄──── all containers (metrics & logs)
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Message Broker | Apache Kafka (KRaft, no ZooKeeper) | 7.5.3 |
| Stream Processing | Apache Spark Structured Streaming | 3.5.0 |
| Data Lake | MinIO (S3-compatible) | latest |
| Warehouse | PostgreSQL | 15 |
| Orchestration | Apache Airflow | 2.8.0 |
| Dashboards | Grafana | latest |
| Metrics | Prometheus + Loki | latest |
| Language | Python | 3.11 |

---

## Project Structure

```
streammart/
├── src/
│   ├── simulator/          # Event generator (Faker-based clickstream)
│   └── spark_jobs/         # 4 PySpark streaming jobs
│       ├── raw_event_writer.py       # Kafka → MinIO Parquet
│       ├── window_aggregator.py      # 1-min windows → PostgreSQL
│       ├── session_tracker.py        # session_window → PostgreSQL
│       └── revenue_aggregator.py     # Hourly revenue → PostgreSQL
├── src/airflow_dags/       # 4 Airflow DAGs for batch processing
├── sql/
│   ├── init_postgres.sql   # Schema DDL (tables, indexes, mat views)
│   └── rollups.sql         # 60-second rollup job (metrics_1min → 5min)
├── config/
│   ├── grafana/            # Provisioned dashboards and datasources
│   └── prometheus/         # Scrape config
├── schemas/                # Avro schemas (for future Schema Registry use)
├── tests/                  # Unit and integration tests
├── docker-compose.yml      # Full stack (3 profiles: default, obs, orchestration)
├── Dockerfile.event-generator
└── .env.example            # Copy to .env and fill in credentials
```

---

## Getting Started

### Prerequisites

- Docker Desktop (≥ 4.x) with at least **12 GB RAM** allocated
- Docker Compose v2
- Git

### Quick Start

```bash
# 1. Clone and configure
git clone <repo-url>
cd streammart
cp .env.example .env
# Edit .env — set POSTGRES_PASSWORD, MINIO_ACCESS_KEY, MINIO_SECRET_KEY

# 2. Start the core pipeline (Kafka, Spark, PostgreSQL, MinIO)
docker compose up -d

# 3. (Optional) Start observability stack (Grafana, Prometheus, Loki)
docker compose --profile obs up -d

# 4. (Optional) Start orchestration stack (Airflow)
docker compose --profile orchestration up -d
```

### Verify the Pipeline is Running

```bash
# Check all core services are healthy
docker compose ps

# Tail Spark logs to confirm events are flowing
docker logs -f streammart-spark-window-aggregator

# Confirm data in PostgreSQL
docker exec -it streammart-postgres psql -U streammart_user -d streammart \
  -c "SELECT event_type, count, window_start FROM metrics_1min ORDER BY window_start DESC LIMIT 10;"

# Check raw Parquet files in MinIO
# Open http://localhost:9001 → user: MINIO_ACCESS_KEY from .env
```

### Access Points

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Spark Master UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | see .env |
| Airflow | http://localhost:8081 | airflow / airflow |
| Prometheus | http://localhost:9090 | — |

---

## Pipeline Deep Dive

### Event Schema

The simulator generates 5 event types:

| Topic | Event | Key Fields |
|---|---|---|
| `events.pageview` | Page visit | session_id, user_id, page_url, device_type |
| `events.product_click` | Product viewed | product_id, product_name, product_price |
| `events.add_to_cart` | Cart action | product_id, quantity, cart_total |
| `events.purchase` | Completed order | order_id, items[], total, payment_method |
| `events.abandonment` | Cart abandoned | cart_total, abandonment_stage |

A 6th topic `events.dlq` (Dead Letter Queue) captures malformed events.

### Spark Streaming Jobs

**Window Aggregator** (`spark_jobs/window_aggregator.py`)
- Reads all 5 event topics
- Computes 1-minute tumbling window aggregations: count, unique sessions, unique users per event type
- Watermark: 2 minutes (handles late data)
- Writes via psycopg2 `ON CONFLICT (window_start, event_type) DO UPDATE` for exact-once semantics on restarts

**Session Tracker** (`spark_jobs/session_tracker.py`)
- Groups events by `session_id` using Spark's `session_window` (30-minute inactivity gap)
- Computes per-session KPIs: funnel counts, conversion flag, revenue, duration
- Writes via psycopg2 `ON CONFLICT (session_id) DO UPDATE`

**Raw Event Writer** (`spark_jobs/raw_event_writer.py`)
- Writes all events as Parquet to MinIO, partitioned by `year/month/day/event_type`
- Snappy compression; immutable audit trail for reprocessing

**Revenue Aggregator** (`spark_jobs/revenue_aggregator.py`)
- Focuses only on `events.purchase`
- Computes hourly revenue, order count, AOV, and product-level sales
- Writes to `product_performance` table

### Batch Layer (Airflow DAGs)

| DAG | Schedule | Purpose |
|---|---|---|
| `streammart_daily_batch_processing` | 02:00 daily | Joins metrics_1min + session_summary → daily_summary; denormalizes product_performance → product_daily_performance |
| `streammart_daily_summary` | 01:00 daily | Revenue rollup → daily_revenue |
| `streammart_data_quality` | Every 6 hours | Completeness, accuracy, timeliness checks |
| `streammart_pipeline_health_check` | Every 15 min | Streaming job health heartbeats |

### Rollup Job

`sql/rollups.sql` runs every 60 seconds inside the `postgres-rollup` container and:
1. Rolls `metrics_1min` → `metrics_5min` (5-minute windowed aggregations)
2. Upserts `daily_revenue` from recent metrics
3. Upserts `product_performance` from recent metrics
4. Inserts a pipeline heartbeat into `pipeline_monitoring`
5. Runs retention deletes (keeps 14 days of 5-min metrics, 3 days of 1-min/sessions)

---

## Design Decisions

**Why KRaft (no ZooKeeper)?**  
Simplifies operations; ZooKeeper was officially deprecated in Kafka 3.x.

**Why `session_window` instead of tumbling window for sessionization?**  
A tumbling window splits sessions that cross fixed boundaries (e.g., 09:45–10:15 would split at 10:00). `session_window` extends the window as long as there's activity within the gap, matching the real semantics of a user session.

**Why psycopg2 instead of JDBC for writes?**  
The Spark JDBC writer uses `mode("append")` which doesn't support `ON CONFLICT`. Since the stream uses `outputMode("update")`, the same window can be emitted multiple times (e.g., after a Spark restart). psycopg2 `executemany` + `ON CONFLICT DO UPDATE` provides true idempotent upserts. Batch sizes are small (≤ 5 rows per trigger), so `.collect()` in the driver is safe.

**Why a separate batch layer alongside streaming?**  
Lambda architecture: streaming gives low-latency approximations; batch gives accurate end-of-day reconciliation. The `daily_summary` computed at 2 AM is authoritative for reporting.

---

## Limitations & Future Improvements

- **Schema Registry**: Avro schemas in `schemas/` are written but the pipeline currently uses JSON. Migrating to Schema Registry would enforce contract validation at the producer.
- **Exactly-once end-to-end**: Kafka → Spark uses at-least-once delivery. psycopg2 upserts make writes idempotent, but duplicate events from the producer are not deduplicated.
- **No secrets manager**: Credentials are passed via environment variables. In production, use Vault or AWS Secrets Manager.
- **Single-node Spark**: The compose setup runs one master + one worker for local dev. A multi-worker setup can be enabled by scaling the `spark-worker` service.
- **Airflow connections**: The `streammart_postgres` Airflow connection must be created manually after first boot (Admin → Connections in the UI).

---

## Running Tests

```bash
# Unit tests (no Docker required)
python -m pytest tests/unit/ -v

# Integration tests (requires running stack)
docker compose up -d
python -m pytest tests/integration/ -v
```

---

## License

MIT

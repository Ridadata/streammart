# StreamMart — Real-Time E-Commerce Analytics Pipeline

[![CI](https://github.com/streammart/streammart/actions/workflows/ci.yml/badge.svg)](.github/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](requirements.txt)

A streaming data pipeline that ingests clickstream events from a simulated e-commerce platform, processes them with Spark Structured Streaming, stores raw data in a MinIO data lake, and surfaces real-time KPIs in Grafana — all orchestrated by Apache Airflow.

> **Note on the CI badge:** it will only resolve once this repository is pushed under a real GitHub org/user — update the badge URL in this file to match your actual `owner/repo` path.

---

## Architecture

```
Event Simulator
     │  (5 event types @ configurable rate — EVENT_GENERATOR_RATE)
     ▼
Kafka (KRaft, 6 topics, 3 partitions each)
     │
     ├──► Spark: Raw Event Writer ──────────────────► MinIO (Parquet data lake)
     │         partitioned by year/month/day/event_type; raw JSON preserved
     │
     ├──► Spark: Window Aggregator ────────────────► PostgreSQL metrics_1min
     │      │                                          1-min tumbling window, 30s trigger
     │      └──────────────────────────────────────► PostgreSQL metrics_5min
     │                                                 5-min tumbling window, 60s trigger
     │                                                 (computed independently from the event
     │                                                 stream — NOT summed from metrics_1min,
     │                                                 see Design Decisions)
     │
     ├──► Spark: Session Tracker ──────────────────► PostgreSQL session_summary
     │         session_window (30-min gap), append-mode output —
     │         a session lands ~40-70 min after its last event (see Design Decisions)
     │
     └──► Spark: Revenue Aggregator ───────────────► PostgreSQL product_performance
               daily product-level revenue (bounded-state daily tumbling window)

PostgreSQL
     │
     ├──► postgres-maintenance (every 60s, sql/maintenance.sql):
     │         DQ heartbeat checks, pipeline heartbeat, retention deletes
     │         (does NOT compute metrics_5min/daily_revenue/product_performance —
     │         each of those has exactly one writer; see Table Ownership below)
     │
     └──► Airflow DAGs (batch/reconciliation layer):
               • daily_batch_processing  @ 02:00 → daily_summary, product_daily_performance
               • daily_summary           @ 01:00 → daily_revenue (authoritative)
               • data_quality            every 6h → data_quality_checks
               • pipeline_health_check   hourly   → connectivity/freshness alerts

Grafana ◄──── PostgreSQL (operational dashboards) + Prometheus (infra metrics)
Prometheus ◄──── kafka-jmx-exporter, kafka-exporter (consumer lag), postgres-exporter,
                 Spark master/worker (built-in PrometheusServlet)
Loki ◄──── promtail ◄──── all container logs
```

---

## Table Ownership

Every table has exactly one writer. This is a project convention, not an accident — an earlier version of this pipeline had two independent processes writing `product_performance` and `daily_revenue`, which is a guaranteed collision. See `CLAUDE.md` for the full rationale.

| Table | Sole Writer | Cadence |
|---|---|---|
| `metrics_1min` | `window_aggregator.py` | ~30s |
| `metrics_5min` | `window_aggregator.py` | ~60s |
| `session_summary` | `session_tracker.py` | ~1min (append-mode) |
| `product_performance` | `revenue_aggregator.py` | ~30s |
| `daily_revenue` | Airflow `daily_summary` DAG | daily @ 01:00 |
| `daily_summary`, `product_daily_performance` | Airflow `daily_batch_processing` DAG | daily @ 02:00 |
| `data_quality_checks` | Airflow `data_quality` DAG (business-rule checks) + `postgres-maintenance` (lightweight heartbeats, distinct `check_name` prefix) | every 6h / every 60s |
| `pipeline_monitoring` | Every DAG + `postgres-maintenance` (append-only event log — intentionally multi-writer, each under its own `job_name`) | varies |

There is deliberately **no `events_raw` table** — raw, per-event data lives in MinIO as Parquet, not Postgres.

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
├── CLAUDE.md               # Architecture reference, table-ownership matrix,
│                            # coding standards, roadmap — read this first
├── src/
│   ├── simulator/           # Event generator (Faker-based clickstream)
│   ├── spark_jobs/          # 4 PySpark Structured Streaming jobs
│   │   ├── raw_event_writer.py       # Kafka → MinIO Parquet (schema-on-read)
│   │   ├── window_aggregator.py      # 1-min AND 5-min windows → PostgreSQL
│   │   ├── session_tracker.py        # session_window (append mode) → PostgreSQL
│   │   └── revenue_aggregator.py     # Daily product revenue → PostgreSQL
│   ├── airflow_dags/        # 4 Airflow DAGs (batch/reconciliation layer)
│   └── utils/                # Shared Postgres/MinIO client helpers
├── sql/
│   ├── init_postgres.sql    # Schema DDL (tables, indexes, mat views)
│   └── maintenance.sql      # 60s heartbeat + retention job (not a rollup —
│                             # see CLAUDE.md for why metrics_5min moved to Spark)
├── config/
│   ├── grafana/              # Provisioned dashboards and datasources
│   ├── prometheus/           # Scrape config, incl. Spark + kafka-exporter targets
│   ├── spark/                # Spark PrometheusServlet metrics config
│   ├── promtail/, loki/      # Log shipping
│   └── jmx/                  # Kafka JMX exporter rules
├── schemas/                  # Avro schemas (drafted, not yet wired into the
│                              # producer/consumers — see Limitations)
├── tests/
│   ├── unit/                 # No Docker required — Spark transform logic +
│   │                          # DAG integrity tests
│   └── integration/          # Requires a running stack; skips gracefully if not
├── .github/workflows/ci.yml  # Lint, unit tests, compose smoke test
├── docker-compose.yml        # Full stack (3 profiles: default, obs, orchestration)
├── Dockerfile.event-generator
└── .env.example               # Copy to .env and fill in credentials
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
# Edit .env — set strong values for POSTGRES_PASSWORD, MINIO_ACCESS_KEY,
# MINIO_SECRET_KEY, AIRFLOW_ADMIN_PASSWORD, GRAFANA_ADMIN_PASSWORD,
# PGADMIN_DEFAULT_PASSWORD. There are no working default credentials —
# every service reads these from .env and fails fast if they're unset.

# 2. Start the core pipeline (Kafka, Spark, PostgreSQL, MinIO, event generator)
docker compose up -d

# 3. (Optional) Start observability stack (Grafana, Prometheus, Loki, kafka-exporter)
docker compose --profile obs up -d

# 4. (Optional) Start orchestration stack (Airflow)
docker compose --profile orchestration up -d
# airflow-init automatically creates the admin user AND the
# streammart_postgres connection every DAG uses — no manual setup step.
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

# Check raw Parquet files are landing in MinIO
# Open http://localhost:9001 → login with MINIO_ACCESS_KEY / MINIO_SECRET_KEY from .env
```

### Access Points

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` from `.env` |
| Kafka UI | http://localhost:8080 | — |
| Spark Master UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` from `.env` |
| Airflow | http://localhost:8085 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env` |
| Prometheus | http://localhost:9090 | — |
| pgAdmin | http://localhost:5050 | `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD` from `.env` |
| Schema Registry | http://localhost:8081 | — |

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

A 6th topic `events.dlq` (Dead Letter Queue) exists in the schema but is not yet a fully wired-up part of the pipeline — see Limitations.

### Spark Streaming Jobs

**Window Aggregator** (`spark_jobs/window_aggregator.py`)
- Reads all 5 event topics
- Computes **two independent** windowed aggregations off the same parsed stream: 1-minute (→ `metrics_1min`, 30s trigger) and 5-minute (→ `metrics_5min`, 60s trigger) — count, unique sessions, unique users per event type
- 5-minute metrics are computed directly from the event stream, not derived from `metrics_1min` — summing five pre-aggregated `approx_count_distinct` values across windows does not produce a valid 5-minute distinct count
- Watermark: 2 minutes (handles late data)
- Writes via psycopg2 `ON CONFLICT (window_start, event_type) DO UPDATE`

**Session Tracker** (`spark_jobs/session_tracker.py`)
- Groups events by `session_id` using Spark's `session_window` (30-minute inactivity gap)
- Computes per-session KPIs: funnel counts, conversion flag, revenue, duration
- Runs in `outputMode("append")` — Spark does not support `update` mode for session-window aggregations. A session is emitted once, after the 40-minute watermark confirms it's closed, so it appears in `session_summary` roughly 40-70 minutes after its last event, not incrementally
- Writes via psycopg2 `ON CONFLICT (session_id) DO UPDATE` (a restart safety net — under normal operation this is effectively insert-only)

**Raw Event Writer** (`spark_jobs/raw_event_writer.py`)
- Writes all events as Parquet to MinIO, partitioned by `year/month/day/event_type`
- Preserves the full raw JSON payload as-is (schema-on-read) rather than exploding it into typed columns, so producer-side field changes never break this job
- Snappy compression; immutable audit trail for reprocessing

**Revenue Aggregator** (`spark_jobs/revenue_aggregator.py`)
- Reads only `events.purchase`, explodes line items
- Computes daily product-level revenue, units sold, and order count, grouped by a real daily tumbling window (not just a derived date column — the watermark can only evict state for a grouping key that includes an actual `window()` column)
- Sole writer of `product_performance`; writes via psycopg2 `ON CONFLICT (product_id, date) DO UPDATE`

### Batch Layer (Airflow DAGs)

| DAG | Schedule | Purpose |
|---|---|---|
| `streammart_daily_batch_processing` | 02:00 daily | Joins metrics_1min + session_summary → daily_summary; denormalizes product_performance → product_daily_performance |
| `streammart_daily_summary` | 01:00 daily | Revenue rollup from session_summary → daily_revenue (authoritative) |
| `streammart_data_quality` | Every 6 hours | Completeness, accuracy, timeliness, and cross-pipeline consistency checks |
| `streammart_pipeline_health_check` | Hourly | Connectivity, freshness, and storage health checks |

All four DAGs use the `streammart_postgres` connection, which `airflow-init` provisions automatically on first boot — no manual setup required.

### Maintenance Job

`sql/maintenance.sql` runs every 60 seconds inside the `postgres-maintenance` container and:
1. Writes lightweight DQ heartbeat checks (distinct from the Airflow DAG's business-rule checks) into `data_quality_checks`
2. Inserts a pipeline heartbeat into `pipeline_monitoring`
3. Runs retention deletes (keeps 14 days of `metrics_5min`, 3 days of `metrics_1min`/`session_summary`)

It does **not** compute `metrics_5min`, `daily_revenue`, or `product_performance` — each of those has exactly one writer elsewhere (see Table Ownership).

---

## Design Decisions

**Why KRaft (no ZooKeeper)?**
Simplifies operations; ZooKeeper was officially deprecated in Kafka 3.x.

**Why `session_window` instead of tumbling window for sessionization?**
A tumbling window splits sessions that cross fixed boundaries (e.g., 09:45–10:15 would split at 10:00). `session_window` extends the window as long as there's activity within the gap, matching the real semantics of a user session. The tradeoff: Spark only supports `append` output mode for session-window aggregations, which means sessions are emitted once (after the watermark confirms closure) rather than incrementally — see Session Tracker above.

**Why psycopg2 instead of JDBC for writes?**
The Spark JDBC writer's `mode("append")` doesn't support `ON CONFLICT`. Every write path in this pipeline that targets a table with a unique/primary key uses psycopg2 `executemany` + `ON CONFLICT DO UPDATE` instead, for true idempotent upserts safe across Spark restarts and reprocessing. Batch sizes are small (bounded by event types, active sessions, or products per trigger), so `.collect()` in the driver is safe.

**Why is `metrics_5min` computed independently instead of rolled up from `metrics_1min`?**
`unique_sessions`/`unique_users` are approximate distinct counts (`approx_count_distinct`). Summing five pre-aggregated 1-minute distinct counts to approximate a 5-minute distinct count is not mathematically valid — a session active across three consecutive 1-minute windows would be counted three times. The only correct fix is computing the 5-minute aggregation directly from the raw event stream, which is what `window_aggregator.py` does.

**Why a separate batch layer alongside streaming?**
Lambda architecture: streaming gives low-latency approximations; batch gives accurate end-of-day reconciliation. `daily_revenue`, computed nightly by Airflow, is authoritative for reporting — the streaming tables are for operational dashboards, not final numbers.

---

## Limitations & Future Improvements

- **Schema Registry**: deployed and running, but the pipeline currently uses JSON, not Avro — the schemas in `schemas/` are drafted but not yet wired into the producer or consumers. See `CLAUDE.md` roadmap.
- **Dead Letter Queue**: the `events.dlq` topic exists but isn't a fully functional DLQ yet — it's only populated on local producer exceptions, and nothing consumes/replays it.
- **Exactly-once end-to-end**: Kafka → Spark uses at-least-once delivery. psycopg2 upserts make writes idempotent, but duplicate events from the producer itself are not deduplicated.
- **No secrets manager**: credentials are passed via environment variables (`.env`, gitignored). In production, use Vault or AWS/GCP Secrets Manager instead.
- **Consumer lag monitoring**: `kafka-exporter` is deployed and correctly configured, but Spark Structured Streaming's Kafka source tracks progress via its own checkpoint files and does not commit consumer offsets to Kafka's `__consumer_offsets` — so `kafka_consumergroup_lag_sum` will not show data for these specific jobs. True lag visibility for Structured Streaming would need a custom `StreamingQueryListener`-based offset committer (e.g. `spark-sql-kafka-offset-committer`) or reading the checkpoint offset log directly.
- **Spark cluster**: runs one master + two workers by default. Scale further by adding more `spark-worker-N` services in `docker-compose.yml`.
- **No benchmarks published yet**: throughput/latency numbers under load haven't been measured and published. Tracked in `CLAUDE.md` roadmap.

---

## Running Tests

```bash
# Unit tests — no Docker required
python -m pip install -r requirements.txt -r requirements-dev.txt
python -m pytest tests/unit -v

# Integration tests — require a running stack; skip automatically if one isn't reachable
docker compose up -d
python -m pytest tests/integration -v
```

CI (`.github/workflows/ci.yml`) runs lint, the full unit test suite (including Airflow DAG integrity checks), and a Docker Compose smoke test that boots a reduced service set and verifies events actually flow end-to-end into `metrics_1min`.

---

## License

MIT — see [LICENSE](LICENSE).

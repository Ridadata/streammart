# CLAUDE.md — StreamMart Engineering Memory

> This file is the single source of truth for any Claude Code session working on this repo.
> Read it before touching code. Update it whenever you complete a roadmap item, change the
> schema, add/remove a service, or discover a new defect. Keep the Roadmap checklist in sync
> with reality — a checked box must mean "verified working," not "code written."

---

## 1. Project Overview

**StreamMart** is a real-time e-commerce analytics pipeline built as a portfolio-grade
demonstration of production data engineering: Kafka ingestion → Spark Structured Streaming →
PostgreSQL (serving layer) + MinIO (data lake) → Grafana dashboards, with Airflow handling the
batch/reconciliation layer and Prometheus/Loki for observability. Everything runs locally via
Docker Compose.

### Purpose

Simulate a realistic e-commerce clickstream (pageviews, product clicks, cart actions, purchases,
abandonment) and process it through a full lambda-architecture pipeline: low-latency streaming
aggregates for real-time dashboards, plus authoritative nightly batch reconciliation.

### Goals

1. **Correctness first.** Every job that runs must produce numbers that are actually true —
   no synthetic rows, no silently-dropped batches, no phantom-column queries.
2. **Portfolio quality.** This repo is meant to be one of the strongest Data Engineering
   portfolio pieces on GitHub — evaluated as if a hiring manager at Databricks/Snowflake/
   Airbnb/Uber/Spotify/Amazon/Microsoft were reviewing it.
3. **It must actually run.** `docker compose up -d` (core profile) should produce a healthy
   stack with real data flowing end-to-end within minutes, verifiable by anyone who clones it.

### Current Implementation Status

**Legend:** ✅ working & verified · ⚠️ partially working / has caveats · ❌ broken · ☠️ dead/unused

This section is the living truth table. Update it the moment you fix or break something —
do not let it drift the way the original README did (that drift was audited and is the reason
this file exists).

| Component | Status | Notes |
|---|---|---|
| Event simulator | ✅ | Realistic funnel simulation, 3 behavior profiles, 40-product catalog |
| Kafka producer | ✅ | JSON payloads, acks=all, snappy compression |
| Spark: Raw Event Writer | 🔧 fixing | `NameError` on `regexp_replace` — see Roadmap Critical #2 |
| Spark: Window Aggregator | ✅ | Only streaming job that ran correctly in the original audit |
| Spark: Session Tracker | 🔧 fixing | `session_window` + `outputMode("update")` unsupported combo |
| Spark: Revenue Aggregator | 🔧 fixing | `.persist()` on streaming df, unbounded state, append-into-PK |
| Airflow: batch_daily_processing | ✅ | Only DAG whose SQL matched the real schema |
| Airflow: daily_summary | 🔧 fixing | Queries phantom `metrics_1min.timestamp/.properties` columns |
| Airflow: data_quality | 🔧 fixing | All 4 tasks query phantom columns |
| Airflow: pipeline_health_check | 🔧 fixing | Queries `is_converted` (real column: `converted`) |
| `sql/rollups.sql` | 🔧 fixing | Inserts a **fabricated** `all-products` row; no date filter on `daily_revenue`; `SUM(distinct)` bug |
| PostgreSQL schema | ✅ | Good indexes; `events_raw` DDL header exists with no table (ghost) |
| MinIO data lake | ❌ | Empty — writer never started due to the NameError above |
| Grafana dashboard | ⚠️ | 8 panels, all Postgres; DLQ panel structurally always empty |
| Prometheus | ⚠️ | Running, scraping little; Spark scrape targets commented out |
| Loki / Promtail | ❌ | Promtail config invalid (`pipeline_stages` at wrong nesting) — fails to start |
| Schema Registry | ☠️ | Deployed, never used — wire format is JSON, Avro schemas unreferenced |
| DLQ (`events.dlq`) | ☠️ | Decorative — only fires on local producer exceptions, nothing consumes it |
| Tests | ❌ | 1 file, tests dataclass constructors only; README-documented dirs don't exist |
| CI/CD | ☠️ | Does not exist |
| Security | ❌ | Password committed in plaintext in `config/grafana/datasources/postgres.yml` |
| Git | ✅ (as of this session) | Repo had no git history before this session |

A full narrative audit (architecture review, severity-rated findings, GitHub-portfolio review,
README review, and the original issue list this roadmap is derived from) was performed and is
condensed into the Roadmap section below. Don't re-derive it — extend it.

### Repository Structure

```
data_eng_project/
├── CLAUDE.md                     # ← you are here
├── README.md                     # Public-facing project README
├── LICENSE                       # MIT
├── docker-compose.yml            # Full stack: core + obs + orchestration profiles
├── Dockerfile.event-generator
├── requirements.txt
├── .env / .env.example
├── src/
│   ├── simulator/
│   │   ├── event_generator.py    # Session-based synthetic event generation
│   │   ├── event_models.py       # Dataclasses, product catalog, behavior profiles
│   │   └── kafka_producer.py     # JSON producer + DLQ hook
│   ├── spark_jobs/
│   │   ├── raw_event_writer.py   # Kafka → MinIO Parquet (data lake)
│   │   ├── window_aggregator.py  # 1-min tumbling windows → metrics_1min
│   │   ├── session_tracker.py    # session_window sessionization → session_summary
│   │   └── revenue_aggregator.py # Purchase events → product_performance
│   ├── airflow_dags/
│   │   ├── batch_daily_processing_dag.py  # 02:00 — daily_summary, product_daily_performance
│   │   ├── daily_summary_dag.py           # 01:00 — daily_revenue
│   │   ├── data_quality_dag.py            # every 6h — completeness/accuracy/timeliness/consistency
│   │   └── pipeline_health_check_dag.py   # hourly — freshness & connectivity checks
│   └── utils/
│       ├── postgres_client.py
│       └── minio_client.py
├── sql/
│   ├── 00_create_airflow_db.sql  # Runs first (alphabetical order in docker-entrypoint-initdb.d)
│   ├── init_postgres.sql         # Full schema DDL
│   └── rollups.sql               # 60s rollup job (metrics_1min → 5min, daily_revenue, DQ heartbeat)
├── schemas/                      # Avro schemas (currently unreferenced by any code)
├── config/
│   ├── grafana/{dashboards,datasources}/
│   ├── prometheus/prometheus.yml
│   ├── loki/loki-config.yml
│   ├── promtail/promtail-config.yml
│   └── jmx/kafka-jmx.yml
├── scripts/                      # Ops scripts (PowerShell + bash + Python)
├── docs/
│   ├── README.md                 # Doc hub
│   ├── design_decisions.md       # Alternatives-considered writeups — strong content, keep
│   └── troubleshooting.md
├── tests/
│   ├── unit/                     # (created this session)
│   └── integration/              # (created this session)
└── .github/workflows/            # CI (added this session)
```

---

## 2. System Architecture

### Data Flow

```
Event Simulator (Python, Faker-based)
     │  5 event types, session-state machine, configurable rate
     ▼
Kafka (KRaft mode, 6 topics, 3 partitions each, key = session_id)
     │
     ├──► Spark: Raw Event Writer ─────────────► MinIO (Parquet)
     │         partitioned by year/month/day/event_type, snappy
     │
     ├──► Spark: Window Aggregator ────────────► Postgres metrics_1min
     │         1-min tumbling windows, watermark 2min, psycopg2 upsert
     │
     ├──► Spark: Session Tracker ──────────────► Postgres session_summary
     │         session_window (30-min gap), psycopg2 upsert
     │
     └──► Spark: Revenue Aggregator ───────────► Postgres product_performance
               windowed purchase aggregation, psycopg2 upsert

PostgreSQL
     │
     ├──► postgres-rollup (every 60s, sql/rollups.sql):
     │         metrics_1min → metrics_5min (incremental upsert)
     │         DQ heartbeat checks
     │         retention deletes
     │
     └──► Airflow DAGs (batch/reconciliation layer):
               • daily_batch_processing @ 02:00 → daily_summary, product_daily_performance
               • daily_summary          @ 01:00 → daily_revenue
               • data_quality           every 6h → data_quality_checks
               • pipeline_health_check  hourly   → freshness/connectivity alerts

Grafana ◄── PostgreSQL (dashboards) + Prometheus (infra metrics)
Prometheus ◄── kafka-jmx-exporter, postgres-exporter, (Spark metrics — pending)
Loki ◄── promtail ◄── all container logs
```

### Table Ownership Matrix

**Rule: every table has exactly one writer.** This was a Critical-severity finding (two
processes wrote `product_performance` and `daily_revenue` independently, guaranteeing
collisions). Do not add a second writer to any row below without updating this table and
removing the old writer first.

| Table | Sole Writer | Cadence | Notes |
|---|---|---|---|
| `metrics_1min` | `window_aggregator.py` | ~30s micro-batch | psycopg2 `ON CONFLICT` upsert |
| `metrics_5min` | `rollups.sql` (postgres-rollup) | 60s | Incremental upsert of last ~15 min only |
| `session_summary` | `session_tracker.py` | 1min micro-batch | psycopg2 `ON CONFLICT` upsert |
| `product_performance` | `revenue_aggregator.py` | ~30s micro-batch | psycopg2 upsert (was: broken JDBC append + fake rollup row) |
| `daily_revenue` | `daily_summary_dag` (Airflow) | 01:00 daily | Authoritative EOD figure; rollup no longer touches this table |
| `daily_summary` | `batch_daily_processing_dag` (Airflow) | 02:00 daily | |
| `product_daily_performance` | `batch_daily_processing_dag` (Airflow) | 02:00 daily | |
| `data_quality_checks` | Airflow `data_quality_dag` | every 6h | Real checks only; `rollups.sql` heartbeat checks are a separate, clearly-named freshness signal |
| `pipeline_monitoring` | Both `rollups.sql` (heartbeat) and Airflow DAGs | 60s / per-DAG-run | Intentional multi-writer: this table is an event log, not a current-state table — each writer inserts its own `job_name`, never updates another's rows |

### Kafka Topics

| Topic | Partitions | Key | Purpose |
|---|---|---|---|
| `events.pageview` | 3 | `session_id` | Page visits |
| `events.product_click` | 3 | `session_id` | Product views |
| `events.add_to_cart` | 3 | `session_id` | Cart additions |
| `events.purchase` | 3 | `session_id` | Completed orders |
| `events.abandonment` | 3 | `session_id` | Abandoned carts |
| `events.dlq` | 3 | `session_id` | Dead-letter queue (being made functional — Roadmap High) |

### Spark Jobs

| Job | Reads | Writes | Trigger | Watermark |
|---|---|---|---|---|
| `raw_event_writer.py` | All 5 event topics | `s3a://raw-events/events/` (Parquet, partitioned) | continuous append | n/a |
| `window_aggregator.py` | All 5 event topics | `metrics_1min` | 30s | 2 min |
| `session_tracker.py` | All 5 event topics | `session_summary` | append mode, finalized sessions only | 1 hour |
| `revenue_aggregator.py` | `events.purchase` | `product_performance` | 30s | 15 min, windowed grouping |

### Airflow DAGs

| DAG ID | Schedule | Catchup | Purpose |
|---|---|---|---|
| `streammart_daily_batch_processing` | `0 2 * * *` | **False** (was `True` — fixed) | daily_summary + product_daily_performance |
| `streammart_daily_summary` | `0 1 * * *` | False | daily_revenue (authoritative) |
| `streammart_data_quality` | `0 */6 * * *` | False | completeness/accuracy/timeliness/consistency checks |
| `streammart_pipeline_health_check` | `0 * * * *` | False | connectivity + freshness alerts |

All DAGs use Postgres connection id **`streammart_postgres`** (unified — previously some used
the nonexistent `postgres_default`).

### PostgreSQL Tables

See `sql/init_postgres.sql` for full DDL. Core tables: `metrics_1min`, `metrics_5min`,
`session_summary`, `product_performance`, `daily_revenue`, `daily_summary`,
`product_daily_performance`, `data_quality_checks`, `pipeline_monitoring`. There is **no**
`events_raw` table — raw events live in MinIO, not Postgres. (An empty DDL header for
`events_raw` existed in the original schema file and caused multiple downstream bugs; it has
been removed.)

### MinIO Layout

```
s3a://raw-events/events/year=YYYY/month=M/day=D/event_type=<type>/*.snappy.parquet
```

### Grafana Dashboards

`config/grafana/dashboards/StreamMart/streammart.json` — single "StreamMart Operational
Dashboard" (uid `streammart_ops`), 8 panels, all Postgres-backed. Prometheus-backed panels
(consumer lag, Spark batch duration, JVM) are Roadmap High-priority additions.

### Prometheus Monitoring

Scrapes: `kafka-jmx-exporter`, `postgres-exporter`. Spark master/worker metrics endpoints are
configured in the Spark services but not yet added as scrape targets. No Alertmanager yet
(Roadmap High).

---

## 3. Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Message Broker | Apache Kafka (KRaft) | 7.5.3 (Confluent) |
| Schema Registry | Confluent Schema Registry | 7.5.3 (deployed, not yet wired in) |
| Stream Processing | Apache Spark Structured Streaming | 3.5.0 |
| Data Lake | MinIO (S3-compatible) | latest |
| Warehouse | PostgreSQL | 15-alpine |
| Orchestration | Apache Airflow | 2.8.0 (python3.11) |
| Dashboards | Grafana | latest |
| Metrics | Prometheus | latest |
| Logs | Loki + Promtail | latest |
| Language | Python | 3.11 |
| Producer client | confluent-kafka | — |

---

## 4. Coding Standards

- **Python**: PEP 8, type hints on new/modified function signatures, docstrings that explain
  *why* (not just what) for non-obvious logic — the existing "WHY THIS EXISTS / WHAT BREAKS
  WITHOUT IT" header pattern in the Spark jobs is good; preserve it in new files.
- **No bare `except Exception: pass`-style swallowing.** A caught write/processing exception
  must either re-raise, or be logged **and** surfaced (metric, alert, or explicit skip-record
  with count) — never silently treated as success. This was a Critical finding in
  `revenue_aggregator.py` and must not recur anywhere else.
- **SQL**: no f-string interpolation of untrusted/dynamic values into queries — use
  parameterized queries (`cursor.execute(sql, params)`) even for internal Airflow context
  values, since the pattern is what reviewers flag regardless of actual exploitability.
- **Every new Postgres-writing job**: use `ON CONFLICT ... DO UPDATE` (psycopg2) for anything
  that can be reprocessed. Plain JDBC `mode("append")` is banned for any table with a unique
  constraint — it doesn't support upsert and will eventually collide.
- **No hardcoded credentials, ever** — not even as a Python default arg fallback value. Read
  from env, fail loudly if absent (see existing `raise RuntimeError(...)` pattern in the Spark
  jobs — that's correct, replicate it).
- **Comments describing a workaround that fabricates or fakes data are forbidden.** If a table
  can't be populated correctly yet, leave it empty and note the gap in this file's status table
  — do not insert placeholder/synthetic rows to make a dashboard look alive.

## 5. Architecture Principles

1. **Single writer per table.** See the ownership matrix above. Before adding a new writer to
   an existing table, either remove the old one or prove (in this file) why concurrent writes
   are safe (e.g., append-only event log tables like `pipeline_monitoring`).
2. **Idempotency over exactly-once theater.** We don't claim Kafka→Spark exactly-once (it's
   at-least-once + idempotent upserts, which is the honest and correct framing — keep it that
   way in docs).
3. **Streaming = fast approximation, Airflow batch = authoritative.** `daily_revenue` etc. from
   the nightly batch DAG are the numbers you'd report externally; streaming metrics are for
   operational dashboards. Don't blur this line.
4. **Bounded state.** Any streaming aggregation must have a watermark **and** a grouping key
   that lets the watermark actually evict state (i.e., group by a windowed column, not a
   derived date/day column alone). Unbounded state growth was a Critical finding.
5. **Fail loud, not quiet.** Missing env vars, failed writes, and DQ check failures should
   raise/alert, not log-and-continue.

## 6. Production Standards

- Secrets only via environment variables / Docker secrets — never committed, never hardcoded
  as a fallback default.
- Every long-running service in `docker-compose.yml` has a `healthcheck` and appropriate
  `depends_on: condition: service_healthy`.
- Retention policy is explicit for every table/topic (see `sql/rollups.sql` retention block and
  Kafka `KAFKA_LOG_RETENTION_HOURS`).
- CI must pass (lint + unit tests + DAG-import check + compose smoke test) before anything is
  considered "done" — see `.github/workflows/`.

## 7. Repository Conventions

- Root-level Markdown is limited to `README.md`, `CLAUDE.md`, `LICENSE`. Everything else lives
  under `docs/`.
- One-off/throwaway scripts do not get committed. If a script is useful long-term, it goes in
  `scripts/` with a comment header explaining when to run it and it must actually work.
- DAG filenames match their DAG ID (`streammart_<name>` ↔ `<name>_dag.py`).
- Env-driven config: anything that varies between dev/prod (rates, memory, hosts, credentials)
  is an env var with a safe example in `.env.example` — never a hardcoded literal in code or
  committed YAML.

## 8. Development Workflow

```bash
# 1. Configure
cp .env.example .env    # fill in real credentials — never commit .env

# 2. Core pipeline
docker compose up -d

# 3. (Optional) Observability
docker compose --profile obs up -d

# 4. (Optional) Orchestration
docker compose --profile orchestration up -d

# 5. Verify
docker compose ps
python -m pytest tests/unit -v
```

Before marking any roadmap item complete: run the relevant service, confirm real data lands
in the target table/bucket, and update both the status table (§1) and the roadmap checklist
(§9) in this file.

---

## 9. Roadmap

Checklist mirrors the audit's Phase 6/7 output. **Check a box only after verifying the fix
actually works against a running stack** — this file exists specifically to prevent the
claims-vs-reality drift that the original README suffered from.

### 🔴 Critical

- [ ] `git init` + baseline commit (repo had no history before this session)
- [ ] Remove hardcoded/committed credentials; env-substitute Grafana Postgres datasource; rotate password
- [ ] Fix `raw_event_writer.py` `NameError` (`regexp_replace` unimported) — revives the data lake
- [ ] Fix `session_tracker.py` `session_window` + `outputMode("update")` incompatibility
- [ ] Fix `revenue_aggregator.py`: drop `.persist()` on streaming df, window the product grouping (bounded state), replace JDBC append with psycopg2 upsert, stop swallowing write exceptions
- [ ] Fix `validate_data.py` syntax error
- [ ] Remove fabricated `all-products` row from `sql/rollups.sql`; add date filter to `daily_revenue` insert; fix `SUM(unique_sessions)` distinct-count bug
- [ ] Rewrite `data_quality_dag.py`, `daily_summary_dag.py`, `pipeline_health_check_dag.py` against the real schema (no more `metrics_1min.timestamp/.properties/.session_id`, `is_converted`); unify Postgres conn id to `streammart_postgres`; set `catchup=False` on the batch DAG
- [ ] Establish single-writer ownership per table (see matrix in §2); remove empty `events_raw` DDL header

### 🟠 High

- [ ] Delete dead code: `fix_dash.py`, broken `.ps1`/`.sh` scripts, unused functions in `raw_event_writer.py`/`revenue_aggregator.py`, unused `src/utils/` (or wire it in properly)
- [ ] Fix `init_kafka_topics.sh` broker address (`kafka:9092` → `kafka:19092`)
- [ ] Fix Promtail config (`pipeline_stages` nesting) so Loki actually receives logs
- [ ] Add Spark + kafka-exporter Prometheus scrape targets; add Prometheus-backed Grafana panels (consumer lag, batch duration, JVM)
- [ ] Wire `EVENT_GENERATOR_RATE` / `LOG_LEVEL` env vars into the event generator (currently ignored)
- [ ] Fix README factual drift (ports 8085/8082, worker count, DAG schedules, test paths); repair corrupted `docs/README.md`
- [ ] Add `LICENSE` file (MIT, as claimed)
- [ ] Build real test suite: unit tests for Spark transforms (local SparkSession), DAG-integrity import test, integration smoke test
- [ ] Add CI/CD (GitHub Actions): lint, unit tests, DAG import check, compose smoke test, badges
- [ ] Add Prometheus alerting rules + Alertmanager (consumer lag, freshness, job-down, DQ failure)

### 🟡 Medium

- [ ] Schema Registry integration: Avro serde in producer + Spark jobs, registered schemas, compatibility mode, evolution demo
- [ ] Functional DLQ: producer/Spark validation routing, quarantine consumer, replay tooling, real dashboard panel
- [ ] Benchmark harness: throughput/latency/lag sweep, results published in README
- [ ] `.collect()`-in-`foreachBatch` scalability note / bound documented explicitly
- [ ] Naive/aware timestamp handling cleanup in `pipeline_health_check_dag.py`
- [ ] Parameterize remaining f-string SQL in DAGs

### 🟢 Low

- [ ] README visual pass: Mermaid architecture diagram, screenshots, hero GIF, badges, ERD
- [ ] ADRs / runbooks (backfill, replay, disaster recovery)
- [ ] Postgres time-based table partitioning
- [ ] Container hardening (non-root, `.dockerignore`, pinned base image digests)
- [ ] TLS / SASL for Kafka, Postgres SSL, per-service least-privilege DB roles

---

## 10. Current Progress

*(Updated each session — see §1 status table for per-component detail.)*

**Completed:** — see §1 as fixes land; this section is deliberately kept in sync with the
checklist above rather than duplicated.

**Technical debt still accepted for now:** Kafka single broker / RF=1 (fine for local dev,
documented as a known limitation, not a bug); Schema Registry running but unused until Medium
roadmap item lands; DLQ topic exists but isn't fully functional until its Medium roadmap item
lands.

**Known issues:** tracked exclusively in §9 — do not maintain a second list.

---

## 11. Portfolio Objective

This repository is being built to be one of the strongest Data Engineering portfolio projects
on GitHub. Every non-trivial decision should be evaluated against:

- **Production readiness** — does it actually run, fail loudly, and recover correctly?
- **Code quality** — would this pass review at a company with a real data platform team?
- **Maintainability** — could a new engineer onboard from this file plus the code alone?
- **Architecture** — is the design defensible, not just functional (Lambda separation,
  idempotency, bounded state, single-writer tables)?
- **Documentation** — do the docs match the code, with evidence (screenshots, benchmarks),
  not just prose claims?
- **Recruiter/hiring-manager impression** — does the repo prove senior judgment in the first
  90 seconds of a review (working `docker compose up`, real dashboard, real numbers)?

When a tradeoff arises, prefer the option that is *provably* correct and *demonstrably*
working over the option that merely looks more feature-complete in a README.

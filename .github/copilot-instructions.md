# Copilot Instructions

This document provides instructions and context to GitHub Copilot to improve its assistance within this project.

## 1. Project Overview

This project is a real-time data engineering pipeline designed to process and analyze a stream of events. It uses a microservices-based architecture orchestrated with Docker Compose.

The pipeline ingests raw events from a generator, processes them through a series of Spark Streaming jobs, stores raw data in a MinIO data lake, and aggregates metrics into a PostgreSQL database.

## 2. Tech Stack

- **Orchestration**: Docker Compose
- **Streaming**: Apache Spark 3.5.0 (Structured Streaming)
- **Message Broker**: Apache Kafka (KRaft mode)
- **Data Lake**: MinIO (S3-compatible object storage)
- **Database**: PostgreSQL
- **Languages**: Python (for the event generator), SQL
- **Infrastructure-as-Code**: Dockerfiles

## 3. Coding Style & Conventions

- **Python**: Follow PEP 8 guidelines. Use type hints where possible.
- **SQL**: Write clean, readable SQL. Use common table expressions (CTEs) for complex queries.
- **Dockerfile**: Keep Dockerfiles minimal and use multi-stage builds if necessary.
- **Shell**: Use `bash` for scripts. Ensure scripts are executable.
- **General**: Keep code well-documented with comments, especially for complex logic.

## 4. Key Architectural Patterns

- **Event-Driven Architecture**: Services communicate asynchronously through Kafka topics.
- **Data Lakehouse**: Raw data is stored in Parquet format in MinIO, while structured, aggregated data resides in PostgreSQL.
- **Microservices**: Each component (event generator, Spark jobs, databases) runs in its own Docker container, promoting separation of concerns.
- **Batch and Stream**: The system combines stream processing (Spark Streaming) with batch-style rollups (SQL jobs in PostgreSQL).

## 5. Important Files

- `docker-compose.yml`: The master file that defines all services, their configurations, and their relationships. This is the single source of truth for the entire stack.
- `.env`: Contains default environment variables for configuring services, such as event rates, memory limits, and connection strings.
- `sql/`: Contains `init_postgres.sql` (schema DDL) and `maintenance.sql` (DQ heartbeat + retention, run every 60s by the `postgres-maintenance` service — this file used to be called `rollups.sql`, see ENGINEERING.md for why it was renamed and trimmed down).
- `src/spark_jobs/`: The four PySpark Structured Streaming jobs (`raw_event_writer.py`, `window_aggregator.py`, `session_tracker.py`, `revenue_aggregator.py`), mounted into the Spark containers and submitted via `spark-submit` per the commands in `docker-compose.yml`.
- `src/airflow_dags/`: The four Airflow DAGs (batch reconciliation, daily summary, data quality, pipeline health check).
- `docs/`: Contains all project documentation, including design decisions and troubleshooting guides.
- `ENGINEERING.md`: Authoritative architecture reference, table-ownership matrix, coding standards, and roadmap. Read it before making non-trivial changes.

## 6. Data Schema

This section details the structure of data as it flows through the pipeline.

### Kafka Topics

The following topics are created by the `kafka-topics-init` service defined in `docker-compose.yml`.

| Topic Name              | Partitions | Replication Factor | Description                               |
| ----------------------- | :--------: | :----------------: | ----------------------------------------- |
| `events.pageview`       |      3     |         1          | User page view events.                    |
| `events.product_click`  |      3     |         1          | User product click events.                |
| `events.add_to_cart`    |      3     |         1          | User "add to cart" events.                |
| `events.purchase`       |      3     |         1          | User purchase events.                     |
| `events.abandonment`    |      3     |         1          | User cart abandonment events.             |
| `events.dlq`            |      3     |         1          | Dead Letter Queue for failed messages.    |

*Event Schema*: The schemas are likely defined by `.avsc` files in a `schemas/` directory. The exact structure for each event is `[TO_FILL]`.

### PostgreSQL Tables

The following tables are defined in `sql/init_postgres.sql`. **There is no `events_raw` table** — raw, unaggregated events are never written to Postgres; they live in MinIO as partitioned Parquet, written by `raw_event_writer.py`. Postgres only holds aggregated/derived tables. Every table has exactly one writer — see the table-ownership matrix in `ENGINEERING.md` before adding a new write path to any of them.

#### `metrics_1min` / `metrics_5min`
Aggregated metrics computed by a Spark job in 1-minute windows.

| Column Name       | Data Type   | Nullable | Description                               |
| :---------------- | :---------- | :------: | :---------------------------------------- |
| `window_start`    | `TIMESTAMPTZ` |    No    | The beginning of the time window.         |
| `window_end`      | `TIMESTAMPTZ` |    No    | The end of the time window.               |
| `event_type`      | `TEXT`      |    No    | The type of event being aggregated.       |
| `count`           | `BIGINT`    |    No    | Total number of events in the window.     |
| `unique_sessions` | `BIGINT`    |    No    | Count of distinct session IDs.            |
| `unique_users`    | `BIGINT`    |    No    | Count of distinct user IDs.               |
| `created_at`      | `TIMESTAMPTZ` |    No    | Timestamp of when the record was created. |
| `updated_at`      | `TIMESTAMPTZ` |    No    | Timestamp of when the record was updated. |

#### `session_summary`
Contains sessionized user behavior and conversion tracking.

| Column Name           | Data Type     | Nullable | Description                                  |
| :-------------------- | :------------ | :------: | :------------------------------------------- |
| `session_id`          | `TEXT`        |    No    | The unique session identifier.               |
| `user_id`             | `TEXT`        |   Yes    | The user identifier, if available.           |
| `start_time`          | `TIMESTAMPTZ` |    No    | The timestamp of the first event in the session. |
| `end_time`            | `TIMESTAMPTZ` |    No    | The timestamp of the last event in the session.  |
| `duration_seconds`    | `INT`         |    No    | The total duration of the session in seconds.  |
| `total_events`        | `INT`         |    No    | Total number of events in the session.       |
| `pageview_count`      | `INT`         |    No    | Number of page views in the session.         |
| `product_click_count` | `INT`         |    No    | Number of product clicks in the session.     |
| `add_to_cart_count`   | `INT`         |    No    | Number of "add to cart" events in the session. |
| `converted`           | `BOOLEAN`     |    No    | True if the session resulted in a purchase.  |
| `revenue`             | `DECIMAL(10,2)` |   Yes    | Total revenue from the session.              |
| `device_type`         | `TEXT`        |   Yes    | The type of device used.                     |
| `created_at`          | `TIMESTAMPTZ` |    No    | Timestamp of when the record was created.    |
| `updated_at`          | `TIMESTAMPTZ` |    No    | Timestamp of when the record was updated.    |

*(Other tables — `product_performance`, `daily_revenue`, `daily_summary`, `product_daily_performance`, `data_quality_checks`, `pipeline_monitoring` — also exist; see `sql/init_postgres.sql` for full DDL and `ENGINEERING.md` for the ownership matrix.)*

### MinIO Buckets

| Bucket Name  | Partitioning Strategy                                     | Description                               |
| :----------- | :-------------------------------------------------------- | :---------------------------------------- |
| `raw-events` | Partitioned by `year`, `month`, `day`, `hour`, `event_type` | Stores raw event data in Parquet format from the `spark-raw-event-writer` job. The exact partitioning is defined in the Spark job logic (`raw_event_writer.py`), which was not analyzed for this summary. |

## 7. Spark Jobs

The following Spark Streaming jobs are defined as services in `docker-compose.yml`. They are submitted to the Spark cluster using `spark-submit` in client deploy mode.

### Job 1: `StreamMart-RawEventWriter`

Reads all 5 event topics and writes them to MinIO for archival and reprocessing, preserving the full raw JSON payload (schema-on-read design — see the module docstring in `raw_event_writer.py`).

| Parameter           | Value                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------- |
| **Purpose**         | Archives all raw event streams to the data lake (MinIO).                                          |
| **Input Source**    | All 5 event topics via `subscribePattern`.                                                        |
| **Output Sink**     | `s3a://raw-events/events/year=/month=/day=/event_type=` in Parquet format.                        |
| **Trigger**         | Continuous processing (default micro-batch trigger), `outputMode("append")`.                      |
| **Checkpoint Path** | `/tmp/checkpoints/raw-event-writer` (inside the container).                                       |
| **Deploy Mode**     | `client`                                                                                          |
| **Executor Memory** | `768M`                                                                                            |
| **Executor Cores**  | `1`                                                                                               |

### Job 2: `StreamMart-WindowAggregator`

Consumes all 5 event topics and runs **two independent** windowed aggregations off the same parsed stream — 1-minute and 5-minute — each writing to its own table. metrics_5min is *not* derived from metrics_1min (summing pre-aggregated approx-distinct counts across windows is mathematically invalid — see the module docstring).

| Parameter           | Value                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------- |
| **Purpose**         | Real-time event counts, unique sessions, unique users per event type, at two granularities.       |
| **Input Source**    | All 5 event topics.                                                                                |
| **Output Sink**     | PostgreSQL tables: `metrics_1min` (30s trigger) and `metrics_5min` (60s trigger).                 |
| **Trigger**         | `outputMode("update")` — valid here since these are time-window (not session-window) aggregations. |
| **Checkpoint Path** | `/tmp/checkpoints/window-aggregator/{1min,5min}` (inside the container).                          |
| **Deploy Mode**     | `client`                                                                                          |
| **Executor Memory** | `768M`                                                                                            |
| **Executor Cores**  | `1`                                                                                               |

### Job 3: `StreamMart-SessionTracker`

Sessionizes events by `session_id` using Spark's `session_window` (30-minute inactivity gap).

| Parameter           | Value                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------- |
| **Purpose**         | Groups events into user sessions and calculates session-level KPIs like duration and conversion.  |
| **Input Source**    | All 5 event topics.                                                                                |
| **Output Sink**     | PostgreSQL table: `session_summary`.                                                              |
| **Trigger**         | `outputMode("append")` — Spark does **not** support `update` mode for `session_window` aggregations. A session is emitted once, ~40-70 min after its last event (bounded by the 40-minute watermark), not incrementally. |
| **Checkpoint Path** | `/tmp/checkpoints/session-tracker` (inside the container).                                        |
| **Deploy Mode**     | `client`                                                                                          |
| **Executor Memory** | `768M`                                                                                            |
| **Executor Cores**  | `1`                                                                                               |

### Job 4: `StreamMart-RevenueAggregator`

Reads only `events.purchase`, explodes line items, and aggregates daily product-level revenue with a bounded-state daily tumbling window (see module docstring for why the grouping key must include a real `window()` column, not just a derived date).

| Parameter           | Value                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------- |
| **Purpose**         | Sole writer of `product_performance` — units sold, revenue, order count per product per day.      |
| **Input Source**    | `events.purchase` only.                                                                            |
| **Output Sink**     | PostgreSQL table: `product_performance` (psycopg2 `ON CONFLICT` upsert, not JDBC append).         |
| **Trigger**         | `outputMode("update")`, 30s trigger.                                                               |
| **Checkpoint Path** | `/tmp/checkpoints/revenue-aggregator` (inside the container).                                     |
| **Deploy Mode**     | `client`                                                                                          |
| **Executor Memory** | `768M`                                                                                            |
| **Executor Cores**  | `1`                                                                                               |

## 8. Service Connection Strings (Internal Docker Network)

| Service         | Internal URL                                              | External URL               |
|-----------------|-----------------------------------------------------------|----------------------------|
| Kafka           | `kafka:19092`                                             | `localhost:9092`           |
| Schema Registry | `http://schema-registry:8081`                             | `http://localhost:8081`    |
| Kafka UI        | —                                                         | `http://localhost:8080`    |
| PostgreSQL      | `postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}` | `localhost:5432` |
| MinIO API       | `http://minio:9000`                                       | `http://localhost:9000`    |
| MinIO Console   | —                                                         | `http://localhost:9001`    |
| Spark Master UI | `http://spark-master:8080`                                | `http://localhost:8082`    |
| Airflow         | —                                                         | `http://localhost:8085`    |
| Grafana         | —                                                         | `http://localhost:3000`    |
| Prometheus      | `http://prometheus:9090`                                  | `http://localhost:9090`    |
| Loki            | `http://loki:3100`                                        | `http://localhost:3100`    |
| pgAdmin         | —                                                         | `http://localhost:5050`    |

**Airflow DB connection:**
`postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/airflow`

---

## 9. Docker Compose Profiles

Services are grouped into optional profiles. Always start core first.

| Profile         | Services included                                      | Command                                        |
|-----------------|--------------------------------------------------------|------------------------------------------------|
| *(default)*     | Kafka, Postgres, MinIO, Spark, Event Generator, Rollup | `docker compose up -d`                         |
| `orchestration` | Airflow (init, webserver, scheduler)                   | `docker compose --profile orchestration up -d` |
| `obs`           | Prometheus, Grafana, Loki, Promtail, JMX Exporter      | `docker compose --profile obs up -d`           |

---

## 10. Anti-Patterns to Avoid

- Do NOT use `localhost` for inter-service communication — always use Docker service hostnames (`kafka`, `postgres`, `minio`)
- Do NOT use `9092` for internal Kafka communication — use `19092` (PLAINTEXT_INTERNAL)
- Do NOT hardcode credentials — always reference `${ENV_VAR}` from `.env`
- Do NOT use RDDs in Spark — use DataFrames only
- Do NOT call `.collect()` on large DataFrames in streaming jobs
- Do NOT manually create Kafka topics — let `kafka-topics-init` handle it
- Do NOT write Spark jobs without checkpoint paths — streaming jobs will lose state on restart
- Do NOT use path-style access=false for MinIO — it requires `path.style.access=true`
- Do NOT start Airflow or monitoring without their profiles — use `--profile orchestration` / `--profile obs`

---

## 11. Common Commands
```bash
# Start core stack
docker compose up -d

# Start with monitoring
docker compose --profile obs up -d

# Start with Airflow
docker compose --profile orchestration up -d

# View Spark job logs
docker logs -f streammart-spark-raw-event-writer
docker logs -f streammart-spark-window-aggregator
docker logs -f streammart-spark-session-tracker

# Check Kafka topics
docker exec -it streammart-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Consume from a topic
docker exec -it streammart-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic events.purchase --from-beginning

# Connect to PostgreSQL
docker exec -it streammart-postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}

# Run maintenance SQL manually (DQ heartbeat + retention; formerly rollups.sql)
docker exec -it streammart-postgres-maintenance \
  psql -h postgres -U ${POSTGRES_USER} -d ${POSTGRES_DB} -f /opt/sql/maintenance.sql

# Restart a failing Spark job
docker compose restart spark-raw-event-writer
```
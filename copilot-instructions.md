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
- `sql/`: This directory contains all SQL scripts, including initialization scripts and rollup/retention jobs.
- `spark-jobs/`: While not present as a directory, the Spark jobs defined within `docker-compose.yml` are central to the data processing logic.
- `docs/`: Contains all project documentation, including design decisions and troubleshooting guides.

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

The following tables are defined in `sql/init_postgres.sql` and `sql/rollups.sql`.

#### `events_raw`
Stores all incoming events with a 3-day retention policy.

| Column Name  | Data Type   | Nullable | Description                               |
| :----------- | :---------- | :------: | :---------------------------------------- |
| `event_id`   | `UUID`      |    No    | Primary Key, auto-generated.              |
| `session_id` | `TEXT`      |    No    | Identifier for the user session.          |
| `user_id`    | `TEXT`      |   Yes    | Identifier for the logged-in user.        |
| `event_type` | `TEXT`      |    No    | The type of event (e.g., 'pageview').     |
| `timestamp`  | `TIMESTAMPTZ` |    No    | Timestamp of when the event occurred.     |
| `properties` | `JSONB`     |    No    | A JSON object with event-specific data.   |
| `created_at` | `TIMESTAMPTZ` |    No    | Timestamp of when the record was created. |

#### `metrics_1min`
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

*(Other tables like `metrics_5min`, `product_performance`, `daily_revenue`, `data_quality_checks`, and `pipeline_monitoring` also exist but are summarized here for brevity.)*

### MinIO Buckets

| Bucket Name  | Partitioning Strategy                                     | Description                               |
| :----------- | :-------------------------------------------------------- | :---------------------------------------- |
| `raw-events` | Partitioned by `year`, `month`, `day`, `hour`, `event_type` | Stores raw event data in Parquet format from the `spark-raw-event-writer` job. The exact partitioning is defined in the Spark job logic (`raw_event_writer.py`), which was not analyzed for this summary. |

## 7. Spark Jobs

The following Spark Streaming jobs are defined as services in `docker-compose.yml`. They are submitted to the Spark cluster using `spark-submit` in client deploy mode.

### Job 1: `StreamMart-RawEventWriter`

This job reads raw events from multiple Kafka topics and writes them to MinIO for archival and batch processing.

| Parameter           | Value                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------- |
| **Purpose**         | Archives all raw event streams to the data lake (MinIO).                                          |
| **Input Source**    | Kafka topics: `events.pageview`, `events.product_click`, `events.add_to_cart`, `events.purchase`. |
| **Output Sink**     | MinIO bucket: `raw-events` in Parquet format.                                                     |
| **Trigger**         | Continuous processing (default micro-batch trigger).                                              |
| **Checkpoint Path** | `/tmp/checkpoints/raw_event_writer` (inside the container).                                       |
| **Deploy Mode**     | `client`                                                                                          |
| **Executor Memory** | `768M`                                                                                            |
| **Executor Cores**  | `1`                                                                                               |

### Job 2: `StreamMart-WindowAggregator`

This job consumes various event streams from Kafka, performs windowed aggregations, and writes the results to PostgreSQL.

| Parameter           | Value                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------- |
| **Purpose**         | Calculates real-time metrics (e.g., event counts, unique users) over tumbling windows.            |
| **Input Source**    | Kafka topics: `events.pageview`, `events.product_click`, etc.                                     |
| **Output Sink**     | PostgreSQL table: `metrics_1min`.                                                                 |
| **Trigger**         | Continuous processing with a likely 1-minute processing time trigger.                             |
| **Checkpoint Path** | `/tmp/checkpoints/window_aggregator` (inside the container).                                      |
| **Deploy Mode**     | `client`                                                                                          |
| **Executor Memory** | `768M`                                                                                            |
| **Executor Cores**  | `1`                                                                                               |

### Job 3: `StreamMart-SessionTracker`

This job is responsible for sessionization—grouping events by user session to track user journeys and calculate session-level metrics.

| Parameter           | Value                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------- |
| **Purpose**         | Groups events into user sessions and calculates session-level KPIs like duration and conversion.  |
| **Input Source**    | Kafka topics: `events.pageview`, `events.product_click`, `events.add_to_cart`, `events.purchase`. |
| **Output Sink**     | PostgreSQL table: `session_summary`.                                                              |
| **Trigger**         | Continuous processing, likely using `flatMapGroupsWithState` for session management.              |
| **Checkpoint Path** | `/tmp/checkpoints/session_tracker` (inside the container).                                        |
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

# Run rollup SQL manually
docker exec -it streammart-postgres-rollup \
  psql -h postgres -U ${POSTGRES_USER} -d ${POSTGRES_DB} -f /opt/sql/rollups.sql

# Restart a failing Spark job
docker compose restart spark-raw-event-writer
```
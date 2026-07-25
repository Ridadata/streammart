# Design Decisions

## Why These Technologies?

This document explains **every technology choice** in StreamMart and the **tradeoffs** involved.

---

## Event Streaming: Why Apache Kafka?

### Alternatives Considered

| Technology | Pros | Cons | When to Use |
|------------|------|------|-------------|
| **Kafka** | High throughput, durable, replay-able | Complex setup, high resource usage | Production, high scale |
| RabbitMQ | Simple, good for queues | Lower throughput, no replay | Task queues, RPC |
| AWS Kinesis | Fully managed, auto-scaling | AWS-only, more expensive | AWS-native apps |
| Redis Streams | Fast, simple | Not durable, limited scale | Caching, real-time |
| Pulsar | Multi-tenancy, geo-replication | Less mature ecosystem | Multi-cloud |

### Why Kafka Won

1. **Industry Standard**: Most companies use Kafka for event streaming
2. **Replay Capability**: Can reprocess historical data (critical for backfills)
3. **Ecosystem**: Rich tooling (Kafka Connect, Schema Registry, Kafka Streams)
4. **Learning Value**: Must-know for data engineers
5. **Free & Open Source**: Apache 2.0 license

### KRaft vs Zookeeper

**Old Way**: Kafka + Zookeeper (2 systems to manage)
```
Kafka Broker ←→ Zookeeper (metadata, leader election)
```

**New Way (KRaft)**: Kafka only (simpler)
```
Kafka Broker (self-managed metadata)
```

**Why KRaft:**
- Simpler deployment (one less service)
- Faster startup (no Zookeeper dependency)
- Better for learning (less complexity)
- Future-proof (Zookeeper deprecated in Kafka 4.0)

---

## Schema Management: Why Avro + Schema Registry?

### Alternatives Considered

| Technology | Pros | Cons | When to Use |
|------------|------|------|-------------|
| **Avro** | Compact, schema evolution | Binary format (not human-readable) | High-volume streams |
| JSON | Human-readable, flexible | Verbose, no schema enforcement | APIs, debugging |
| Protobuf | Fast, strongly-typed | Steeper learning curve | gRPC, microservices |
| MessagePack | Fast, compact | Limited schema support | Performance-critical |

### Why Avro Won

1. **Schema Evolution**: Add fields without breaking consumers (backward/forward compatible)
2. **Compact**: 30-50% smaller than JSON (saves storage & bandwidth)
3. **Kafka Integration**: Confluent Schema Registry is Kafka-native
4. **Type Safety**: Compile-time schema validation prevents bugs

**Example: Schema Evolution**
```avro
// Version 1
{"name": "PageView", "fields": [
  {"name": "url", "type": "string"},
  {"name": "timestamp", "type": "long"}
]}

// Version 2 (add optional field - backward compatible)
{"name": "PageView", "fields": [
  {"name": "url", "type": "string"},
  {"name": "timestamp", "type": "long"},
  {"name": "device", "type": ["null", "string"], "default": null}
]}
```

Old consumers can read new data (ignore `device`).

---

## Stream Processing: Why Spark Structured Streaming?

### Alternatives Considered

| Technology | Pros | Cons | When to Use |
|------------|------|------|-------------|
| **Spark SS** | SQL-like API, exactly-once, mature | Resource-heavy, Java/Scala | Large-scale, complex logic |
| Kafka Streams | Native Kafka, lightweight | Kafka-only, Java DSL | Simple transformations |
| Flink | True streaming, low latency | Smaller ecosystem, complex | Sub-second latency needs |
| Storm | Low latency | At-least-once only | Legacy systems |
| Beam (Dataflow) | Portable, unified batch/stream | GCP-centric, verbose API | Multi-cloud portability |

### Why Spark Won

1. **Exactly-Once Semantics**: Built-in idempotent writes
2. **SQL API**: Easy to learn (if you know SQL, you can use Spark)
3. **Unified Batch + Stream**: Same code for historical reprocessing
4. **Ecosystem**: Integrates with everything (Kafka, S3, PostgreSQL, etc.)
5. **Mature**: Battle-tested at Uber, Netflix, Apple

**Structured Streaming vs DStreams (old Spark API)**

| Feature | DStreams | Structured Streaming |
|---------|----------|---------------------|
| API | RDD-based | DataFrame-based (SQL) |
| Exactly-once | Manual | Built-in |
| Watermarking | Manual | Built-in |
| Learning curve | Steeper | Gentler |
| **Recommendation** | **Legacy** | **Use this** |

---

## Storage: Why MinIO + PostgreSQL?

### MinIO (Data Lake - Raw Events)

**Alternative**: AWS S3

| | MinIO | AWS S3 |
|---|-------|--------|
| Cost | Free (self-hosted) | ~$0.023/GB/month |
| Compatibility | S3 API (portable) | S3 API |
| Performance | Local (fast) | Network (latency) |
| Scale | Limited by hardware | Unlimited |
| **Use Case** | **Development, on-prem** | **Production, cloud** |

**Why MinIO for this project:**
- 100% S3-compatible (code works on S3 with 1-line change)
- Free, runs locally
- Demonstrates object storage patterns

**When to use S3:**
- Production workloads
- Need >1PB storage
- Multi-region replication

### PostgreSQL (Gold Layer - Aggregated Metrics)

**Alternatives**: MySQL, Redshift, BigQuery, TimescaleDB

| | PostgreSQL | Redshift | BigQuery |
|---|------------|----------|----------|
| Cost | Free | $$$$ | $$$ |
| Query Speed | Fast (for GB) | Fast (for TB+) | Fastest (for PB) |
| Scale | Vertical (~10TB) | Horizontal (PB+) | Horizontal (PB+) |
| ACID | Yes | Yes | Eventually |
| **Use Case** | **OLTP + light analytics** | **Enterprise DW** | **Cloud-native DW** |

**Why PostgreSQL Won:**
1. **Free & Popular**: Most widely-used open-source DB
2. **Feature-Rich**: JSONB, full-text search, CTEs, window functions
3. **Strong ACID**: Guarantees data consistency
4. **Good for Dashboards**: Fast queries on aggregated data (~millions of rows)
5. **Easy Setup**: Single Docker container

**When to upgrade:**
- Data > 10TB? → Redshift/BigQuery
- Time-series only? → TimescaleDB
- Multi-region writes? → PostgreSQL + Citus

---

## Orchestration: Why Apache Airflow?

### Alternatives Considered

| Technology | Pros | Cons | When to Use |
|------------|------|------|-------------|
| **Airflow** | Mature, Python, rich UI | Resource-heavy, complex for simple jobs | Complex DAGs |
| Prefect | Modern, easy, Pythonic | Smaller ecosystem | Python-first teams |
| Dagster | Software-defined assets, typed | Newer, less adoption | Data-as-code philosophy |
| cron | Simple, universally available | No dependencies, no retry logic | Simple scheduled tasks |
| Luigi (Spotify) | Simple, Python | Less active | Legacy |

### Why Airflow Won

1. **Industry Standard**: Used by Airbnb, Uber, Twitter, Lyft
2. **Rich UI**: Visual DAGs, log viewing, task retries
3. **Extensibility**: 1000+ integrations (providers)
4. **Backfilling**: Rerun historical DAGs easily
5. **Community**: Large, active community

**Key Airflow Concepts in StreamMart:**

- **DAG**: Directed Acyclic Graph (workflow definition)
- **Task**: Single unit of work (Python function, SQL query)
- **Operator**: Template for creating tasks (PythonOperator, BashOperator)
- **Schedule**: Cron expression (`0 1 * * *` = daily at 1 AM)
- **Idempotency**: Running twice = same result as once

---

## Monitoring: Why Prometheus + Grafana?

### Alternatives Considered

| Technology | Pros | Cons | When to Use |
|------------|------|------|-------------|
| **Prometheus + Grafana** | Open-source, CNCF standard | Manual setup | Self-hosted, Kubernetes |
| Datadog | All-in-one, great UX | Expensive ($$$) | Enterprise, cloud |
| New Relic | APM + logs + metrics | Expensive ($$) | Application monitoring |
| CloudWatch (AWS) | Native AWS integration | AWS-only, limited queries | AWS workloads |
| ELK Stack | Logs + metrics + search | Complex, resource-heavy | Log-heavy workloads |

### Why Prometheus + Grafana Won

1. **Free**: Both open-source
2. **Pull-Based**: Prometheus scrapes metrics (resilient to failures)
3. **PromQL**: Powerful query language (`rate(events_total[1m])`)
4. **Alerting**: Alert on anomalies (Alertmanager)
5. **Grafana**: Beautiful, customizable dashboards

**Prometheus Data Model:**
```
metric_name{label1="value1", label2="value2"} value timestamp

Example:
kafka_messages_per_sec{topic="events.pageview", partition="0"} 342 1234567890
```

---

## Language Choice: Why Python?

### Alternatives Considered

| Language | Pros | Cons | When to Use |
|----------|------|------|-------------|
| **Python** | Easy, huge ecosystem, data science | Slower, GIL | Data pipelines, ML |
| Java/Scala | Fast, JVM (Kafka/Spark native) | Verbose, steep curve | Performance-critical |
| Go | Fast, concurrency, simple | Smaller data ecosystem | Microservices, CLIs |
| Rust | Safest, fastest | Steep curve, small ecosystem | Systems programming |

### Why Python Won

1. **Ease of Learning**: Most accessible for beginners
2. **Data Ecosystem**: pandas, Faker, psycopg2, etc.
3. **Spark Support**: PySpark is first-class (not just an afterthought)
4. **Airflow**: Native Python (DAGs are Python scripts)
5. **Job Market**: Most data engineer roles use Python

**Performance Note:**
- Python is "slow" for computation
- But in data engineering, bottleneck is usually I/O (network, disk)
- Spark runs on JVM (Python is just the API)

---

## What We Deliberately Left Out

### Not Included (And Why)

1. **Kubernetes**: Too complex for local development (use Docker Compose)
2. **Delta Lake**: Overkill for this scale (use plain Parquet)
3. **ata Catalog (Hive Metastore**: Not needed for small data
4. **ML Integration**: Out of scope (StreamMart is about data engineering)
5. **Real-time Alerts**: Use Prometheus Alertmanager (left as extension)
6. **CI/CD**: Left as extension (focus on pipeline, not DevOps)

### Production Additions

For real production, you'd add:
- Authentication & authorization (Kerberos, LDAP)
- Encryption (TLS for all communication)
- High availability (multi-node everything)
- Disaster recovery (backups, replication)
- Cost monitoring (track spend per service)

---

## Key Takeaway

**StreamMart demonstrates production patterns using free, open-source tools.**

Every technology choice is:
1. **Learnable**: Concepts transfer to paid alternatives
2. **Scalable**: Can grow to production scale
3. **Realistic**: Used by real companies

---

**Next**: See [troubleshooting.md](troubleshooting.md) for common issues and fixes.

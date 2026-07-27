# StreamMart Docs

## Read First

1. [../README.md](../README.md): project overview, key features, quick start
2. [../ARCHITECTURE.md](../ARCHITECTURE.md): full system diagram, table ownership, every job/DAG in detail
3. [../DESIGN_DECISIONS.md](../DESIGN_DECISIONS.md): technology alternatives considered and why
4. [../RUNBOOK.md](../RUNBOOK.md): command-by-command first-run guide with real output
5. [../CLAUDE.md](../CLAUDE.md): current implementation status and roadmap
6. [troubleshooting.md](troubleshooting.md): common failures and fixes

## Quick Start (Low Resource Mode)

Run only core services by default:

```bash
docker compose up -d --build
```

Optional profiles:

```bash
docker compose --profile obs up -d --force-recreate  # Grafana/Prometheus/Loki
docker compose --profile orchestration up -d          # Airflow
```

If Docker reports a missing network ID (for example: `failed to set up container networking: network <id> not found`), run:

```bash
# Recreate only observability containers with fresh network bindings
docker rm -f streammart-prometheus streammart-loki streammart-promtail streammart-grafana streammart-postgres-exporter streammart-kafka-jmx-exporter streammart-kafka-exporter
docker compose --profile obs up -d --force-recreate
```

## Low-Resource Tuning

If running on a machine with limited RAM/CPU:

- Lower the event generation rate via `EVENT_GENERATOR_RATE` in `.env`
- Lower Spark executor/driver memory (`SPARK_DRIVER_MEMORY`, `SPARK_EXECUTOR_MEMORY` in `.env`, and the `mem_limit` values in `docker-compose.yml`)
- Kafka retention is already reduced (`KAFKA_LOG_RETENTION_HOURS: 24`) for local SSD safety
- The `postgres-maintenance` service (see `sql/maintenance.sql`) writes lightweight DQ heartbeat and pipeline-monitoring rows every 60 seconds regardless of load, so those tables stay active even at low event rates

## Postgres Tables Kept Continuously Active

- `metrics_1min`, `metrics_5min` — Spark, ~30-60s
- `session_summary` — Spark, ~40-70min latency (append-mode; see `../README.md` Design Decisions)
- `product_performance` — Spark, ~30s
- `data_quality_checks`, `pipeline_monitoring` — `postgres-maintenance`, every 60s

`daily_revenue`, `daily_summary`, `product_daily_performance` are populated once daily by Airflow — see `../README.md`.

## Verify Health

```bash
docker compose ps
docker compose exec -T postgres psql -U streammart_user -d streammart -c "SELECT COUNT(*) FROM metrics_1min;"
docker compose exec -T postgres psql -U streammart_user -d streammart -c "SELECT COUNT(*) FROM session_summary;"
```

### Weekly Checks

- [ ] Review Grafana dashboards for anomalies
- [ ] Check `docker compose logs` across services for recurring errors
- [ ] Confirm all four Airflow DAGs have recent successful runs

### Monthly Checks

- [ ] Capacity planning (resource utilization trends)
- [ ] Data quality audit
- [ ] Update dependencies (security patches)
- [ ] Review and optimize slow queries

---

## Common Pitfalls

### ❌ Running Spark in local mode
```bash
# Wrong:
--master local[*]

# Right:
--master spark://spark-master:7077
```

### ❌ Forgetting to create Kafka topics
**Symptom:** Spark jobs fail with "topic does not exist"
**Fix:** `docker compose run --rm kafka-topics-init` (see [troubleshooting.md](troubleshooting.md))

### ❌ Not initializing PostgreSQL tables
**Symptom:** No data in database even though Spark is running
**Fix:** Confirm `sql/init_postgres.sql` ran on first boot — check `docker compose logs postgres` for "database initialization completed"; if the `postgres-data` volume already existed from a prior run, init scripts don't re-run (Postgres only runs `docker-entrypoint-initdb.d` on a fresh, empty data directory)

### ❌ Over-allocating resources
**Symptom:** Jobs stuck in WAITING state
**Formula:** Ensure (jobs × memory) < worker capacity

---

## Performance Tuning

### Kafka

```bash
# Increase partitions for higher throughput
docker compose exec kafka kafka-topics --alter --topic events.pageview --partitions 6
```

### Spark

```python
# Tune micro-batch interval (see each job's write_to_postgres/write_product_performance
# function for its current trigger interval)
.trigger(processingTime="5 seconds")

# Increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "20")  # Project default: 3, tuned for local dev
```

### PostgreSQL

```sql
-- Add indexes on frequently queried columns (adjust to your actual query patterns)
CREATE INDEX idx_session_summary_start_time ON session_summary(start_time DESC);
CREATE INDEX idx_product_performance_date ON product_performance(date DESC);
```

---

## Contributing to Documentation

### Documentation Standards

- Use Markdown for all docs
- Include code examples where relevant
- Keep guides under 500 lines (split if too long)
- Use proper heading hierarchy (# → ## → ###)
- Link between documents with relative paths, and verify the link target actually exists

### File Organization

```
Root level:
├── README.md              # Overview, key features, quick start
├── ARCHITECTURE.md         # Full system diagram, table ownership, job/DAG detail
├── DESIGN_DECISIONS.md     # Technology alternatives considered and why
├── RUNBOOK.md              # Command-by-command first-run guide
├── CLAUDE.md               # Implementation status and roadmap
├── LICENSE

docs/ folder:
├── README.md              # This file — docs hub
├── images/                # Screenshots and demo GIF (see images/README.md)
└── troubleshooting.md     # Error diagnosis
```

### Updates Needed

If you modify the project:
- [ ] Update `ARCHITECTURE.md` if the data flow, table ownership, or any job's settings change
- [ ] Update `README.md`'s Project Metrics if the counts (services, topics, jobs, DAGs, tables, panels, tests) change
- [ ] Update `CLAUDE.md`'s status table and roadmap checklist
- [ ] Update `docs/troubleshooting.md` if a new failure mode is discovered
- [ ] Verify every relative link in the doc you're editing still resolves — this file has previously drifted to link at files that were never created

---

## External Resources

### Learn More About Technologies

- **Kafka:** [Official Docs](https://kafka.apache.org/documentation/) | [Confluent Tutorials](https://developer.confluent.io/)
- **Spark:** [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- **Airflow:** [Apache Airflow Docs](https://airflow.apache.org/docs/)
- **Docker:** [Docker Compose Reference](https://docs.docker.com/compose/)

### Similar Projects

- [Yelp's Data Pipeline](https://engineeringblog.yelp.com/2016/07/billions-of-messages-a-day-yelps-real-time-data-pipeline.html)
- [Uber's Real-Time Analytics](https://www.uber.com/blog/real-time-exactly-once-ad-event-processing/)
- [Confluent Reference Architecture](https://www.confluent.io/resources/kafka-the-definitive-guide-v2/)

---

## Support & Contact

**Questions?** Open an issue on GitHub or check existing issues for solutions.

**Found a bug?** Please report with:
- Steps to reproduce
- Expected vs actual behavior
- Error logs (from `docker compose logs <service>`)
- System info (OS, Docker version, RAM/CPU)

---

**Current implementation status, technical debt, and roadmap:** see [../CLAUDE.md](../CLAUDE.md) — kept up to date as the project evolves, rather than duplicating a status snapshot here that would just go stale again.

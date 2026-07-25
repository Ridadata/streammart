# StreamMart Docs (Clean Version)

This project now keeps documentation inside this folder only.

## Read First

1. [docs/README.md](README.md): quick start and operational defaults
2. [docs/troubleshooting.md](troubleshooting.md): common failures and fixes
3. [docs/design_decisions.md](design_decisions.md): architecture and tradeoffs

## Quick Start (Low Resource Mode)

Run only core services by default:

```bash
docker compose up -d --build
```

Optional profiles:

```bash
docker compose --profile obs up -d --force-recreate  # Grafana/Prometheus
docker compose --profile orchestration up -d # Airflow
```

If Docker reports a missing network ID (for example: `failed to set up container networking: network <id> not found`), run:

```bash
# Recreate only observability containers with fresh network bindings
docker rm -f streammart-prometheus streammart-loki streammart-promtail streammart-grafana streammart-postgres-exporter streammart-kafka-jmx-exporter
docker compose --profile obs up -d --force-recreate
```

## What Changed For Stability

- Lower event generation rate (`EVENT_GENERATOR_RATE=8`)
- Lower Spark/Kafka memory footprint
- Kafka retention reduced for SSD safety
- Auto-rollup service fills summary tables every minute

## Postgres Tables Now Kept Active

- `metrics_1min`
- `metrics_5min`
- `session_summary`
- `product_performance`
- `daily_revenue`
- `data_quality_checks`
- `pipeline_monitoring`

## Verify Health

```bash
docker compose ps
docker compose exec -T postgres psql -U streammart_user -d streammart -c "SELECT COUNT(*) FROM metrics_1min;"
docker compose exec -T postgres psql -U streammart_user -d streammart -c "SELECT COUNT(*) FROM daily_revenue;"
```
- [ ] Review Grafana dashboards

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
**Fix:** Run Step 3 of [DEPLOYMENT_GUIDE.md](../DEPLOYMENT_GUIDE.md)

### ❌ Not initializing PostgreSQL tables
**Symptom:** No data in database even though Spark is running  
**Fix:** Run Step 2 of [DEPLOYMENT_GUIDE.md](../DEPLOYMENT_GUIDE.md)

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
# Tune micro-batch interval
.trigger(processingTime="5 seconds")  # Default: 2 seconds

# Increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "20")  # Default: 200
```

### PostgreSQL

```sql
-- Add indexes on frequently queried columns
CREATE INDEX idx_metrics_window_end ON metrics_1min(window_end DESC);
CREATE INDEX idx_events_timestamp ON events_raw(timestamp DESC);
```

---

## Contributing to Documentation

### Documentation Standards

- Use Markdown for all docs
- Include code examples where relevant
- Keep guides under 500 lines (split if too long)
- Use proper heading hierarchy (# → ## → ###)
- Link between documents (relative paths)

### File Organization

```
Root level: User-facing guides
├── README.md                 # Entry point
├── DEPLOYMENT_GUIDE.md       # How to deploy
├── VERIFICATION_GUIDE.md     # How to verify
├── ARCHITECTURE.md           # System design
├── PRODUCTION_CONFIG.md      # Production best practices

docs/ folder: Supporting docs
├── README.md                 # This file (doc hub)
├── design_decisions.md       # Why we chose X over Y
└── troubleshooting.md        # Error diagnosis
```

### Updates Needed

If you modify the project:
- [ ] Update README.md if architecture changes
- [ ] Update DEPLOYMENT_GUIDE.md if steps change
- [ ] Update VERIFICATION_GUIDE.md if new checks needed
- [ ] Update ARCHITECTURE.md if new services added

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

**Last Updated:** 2026-01-06  
**Documentation Version:** 1.0  
**Project Status:** Production-ready demo

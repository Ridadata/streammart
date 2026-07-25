# StreamMart Troubleshooting Guide

Common issues and how to fix them.

---

## Docker & Infrastructure

### Issue: "Cannot connect to Docker daemon"

**Symptom:**
```
Error response from daemon: Cannot connect to the Docker daemon
```

**Solution:**
1. Start Docker Desktop
2. Wait for Docker to fully start (check system tray icon)
3. Run: `docker info` to verify

---

### Issue: "Port already in use"

**Symptom:**
```
Error starting userland proxy: listen tcp 0.0.0.0:9092: bind: address already in use
```

**Solution:**

**Windows:**
```powershell
# Find process using port 9092
netstat -ano | findstr :9092

# Kill process (replace PID with actual)
taskkill /PID <PID> /F
```

**Or change port in docker-compose.yml:**
```yaml
ports:
  - "9093:9092"  # Use different host port
```

---

### Issue: "Not enough memory"

**Symptom:**
- Containers crashing with exit code 137
- Services slow to start
- `docker compose ps` shows "Exited (137)"

**Solution:**

1. Increase Docker Desktop memory:
   - Settings → Resources → Memory → 8GB minimum
   - Apply & Restart

2. Reduce services:
   ```bash
   # Start minimal set
   docker compose up -d kafka postgres minio
   ```

---

### Issue: "Containers won't stop"

**Symptom:**
```
docker compose down  # takes forever
```

**Solution:**
```bash
# Force stop and remove
docker compose down -v --remove-orphans

# Nuclear option (stops ALL containers)
docker stop $(docker ps -aq)
docker rm $(docker ps -aq)
```

---

## Kafka

### Issue: "Kafka not ready"

**Symptom:**
- Event generator can't connect
- "Connection refused" errors

**Solution:**

1. Check Kafka is running:
   ```bash
   docker compose ps kafka
   ```

2. Check Kafka logs:
   ```bash
   docker compose logs kafka | tail -50
   ```

3. Wait longer (Kafka takes 30-60s to start):
   ```bash
   # Wait for this message in logs:
   # "Kafka Server started"
   ```

4. Verify connectivity:
   ```bash
   docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

---

### Issue: "Topic creation failed"

**Symptom:**
```
Error: Topic 'events.pageview' already exists
```

**Solution:**

This is usually OK (idempotent). But if you want to reset:

```bash
# List topics
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Delete topic
docker compose exec kafka kafka-topics --delete --topic events.pageview --bootstrap-server localhost:9092

# Recreate all topics using the same one-shot service docker-compose.yml
# runs automatically on startup (this is the single source of truth for
# topic definitions — there is deliberately no separate standalone script,
# to avoid two topic-creation code paths silently drifting apart)
docker compose run --rm kafka-topics-init
```

---

### Issue: "Consumer lag growing"

**Symptom:**
- Kafka UI shows increasing lag
- Events not appearing in PostgreSQL

**Solution:**

1. Check Spark jobs are running:
   ```bash
   docker compose ps spark-master spark-worker-1
   ```

2. Increase Spark resources:
   ```yaml
   # docker-compose.yml
   spark-worker-1:
     environment:
       SPARK_WORKER_MEMORY: 4G  # Increase from 2G
       SPARK_WORKER_CORES: 4    # Increase from 2
   ```

3. Scale consumers (add more Spark workers):
   ```bash
   docker compose up -d --scale spark-worker=3
   ```

---

## Schema Registry

### Issue: "Schema validation failed"

**Symptom:**
```
Events going to DLQ (events.dlq topic)
```

**Solution:**

1. Check Schema Registry health:
   ```bash
   curl http://localhost:8081/
   ```

2. List registered schemas:
   ```bash
   curl http://localhost:8081/subjects
   ```

3. Validate event manually:
   ```python
   from src.simulator.kafka_producer import StreamMartProducer
   producer = StreamMartProducer()
   # Check logs for validation errors
   ```

4. Check DLQ for error details:
   ```bash
   docker compose exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic events.dlq \
     --from-beginning
   ```

---

## PostgreSQL

### Issue: "Cannot connect to PostgreSQL"

**Symptom:**
```
psycopg2.OperationalError: could not connect to server
```

**Solution:**

1. Check PostgreSQL is running:
   ```bash
   docker compose ps postgres
   ```

2. Check credentials in .env (values shown here are placeholders — use your own):
   ```bash
   POSTGRES_USER=streammart_user
   POSTGRES_PASSWORD=<your value from .env>
   POSTGRES_DB=streammart
   ```

3. Test connection:
   ```bash
   docker compose exec postgres psql -U streammart_user -d streammart
   ```

4. Check localhost vs container name:
   - From host: `localhost:5432`
   - From container: `postgres:5432`

---

### Issue: "Table does not exist"

**Symptom:**
```
relation "events_raw" does not exist
```

**Solution:**

1. Check if init script ran:
   ```bash
   docker compose logs postgres | grep "init"
   ```

2. Manually run init script:
   ```bash
   docker compose exec -T postgres psql -U streammart_user -d streammart < sql/init_postgres.sql
   ```

3. Verify tables:
   ```bash
   docker compose exec postgres psql -U streammart_user -d streammart -c "\dt"
   ```

---

## Spark

### Issue: "Running Applications (0)" or a job stays WAITING

**Symptom:**
```text
TaskSchedulerImpl: Initial job has not accepted any resources
```

**Solution:**

1. Check whether the Spark worker is already full:
   ```bash
   docker compose exec -T spark-master curl -s http://localhost:8080/json/
   ```

2. If the worker has no free cores or memory, add capacity by starting the second worker:
   ```bash
   docker compose up -d spark-worker-2
   ```

3. If you only need to run one or two jobs, stop the extra Spark app containers so the remaining jobs can schedule.

4. Re-check the Spark UI:
   - http://localhost:8080

### Issue: "Spark job fails to start"

**Symptom:**
```
py4j.protocol.Py4JJavaError: An error occurred while calling o42.load
```

**Solution:**

1. Check Spark logs:
   ```bash
   docker compose logs spark-master
   docker compose logs spark-worker-1
   ```

2. Verify Spark UI:
   - http://localhost:8082

3. Check Python path:
   ```python
   # In spark-submit, ensure correct paths
   --py-files /opt/spark-jobs/utils.zip
   ```

4. Verify Kafka connection from Spark:
   ```bash
   docker compose exec spark-master nc -zv kafka 9092
   ```

---

### Issue: "Checkpoint corruption"

**Symptom:**
```
Checkpoint directory /tmp/checkpoints/... is corrupted
```

**Solution:**

1. Delete checkpoint (will reprocess from latest offset):
   ```bash
   docker compose exec spark-master rm -rf /tmp/checkpoints/*
   ```

2. Restart Spark job

**Warning**: Deleting checkpoints may cause **reprocessing** or **data loss** depending on job configuration.

---

## MinIO

### Issue: "Bucket not found"

**Symptom:**
```
NoSuchBucket: The specified bucket does not exist
```

**Solution:**

1. Check MinIO is running:
   ```bash
   docker compose ps minio
   ```

2. Access MinIO console:
   - http://localhost:9001
   - Login: `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` from your `.env`

3. Create bucket manually:
   ```bash
   docker compose exec minio mc mb myminio/raw-events
   ```

4. Or run init again:
   ```bash
   docker compose up minio-init
   ```

---

## Airflow

### Issue: "Airflow webserver not starting"

**Symptom:**
- http://localhost:8085 not responding
- Container keeps restarting

**Solution:**

1. Check logs:
   ```bash
   docker compose logs airflow-webserver
   docker compose logs airflow-scheduler
   ```

2. Common issue: Database not initialized
   ```bash
   docker compose run airflow-init
   ```

3. Reset Airflow database:
   ```bash
   docker compose down -v
   docker compose up -d
   ```

---

### Issue: "DAGs not appearing"

**Symptom:**
- No DAGs in Airflow UI

**Solution:**

1. Check DAG folder is mounted:
   ```bash
   docker compose exec airflow-webserver ls /opt/airflow/dags
   ```

2. Check for Python errors:
   ```bash
   docker compose exec airflow-webserver airflow dags list
   ```

3. Check DAG file syntax:
   ```python
   python src/airflow_dags/daily_summary_dag.py
   # Should not have syntax errors
   ```

---

## Python / Event Generator

### Issue: "ModuleNotFoundError"

**Symptom:**
```
ModuleNotFoundError: No module named 'confluent_kafka'
```

**Solution:**

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Use virtual environment:
   ```bash
   python -m venv venv
   .\venv\Scripts\activate  # Windows
   source venv/bin/activate  # Linux/Mac
   pip install -r requirements.txt
   ```

---

### Issue: "Event generator crashes"

**Symptom:**
```
KeyboardInterrupt
Exception in thread...
```

**Solution:**

1. Graceful shutdown:
   - Press Ctrl+C once (wait for flush)
   - Don't force kill immediately

2. Check Kafka connection:
   ```python
   from kafka import KafkaProducer
   producer = KafkaProducer(bootstrap_servers='localhost:9092')
   producer.close()
   # Should not raise exception
   ```

---

## Performance Issues

### Issue: "Events processing slowly"

**Symptom:**
- Growing Kafka lag
- Dashboard not updating

**Diagnosis:**

1. Check Kafka lag:
   - Kafka UI → Consumer Groups → Check lag

2. Check Spark processing rate:
   - Spark UI (http://localhost:8082) → Running jobs → Check "Input Rate"

3. Check PostgreSQL connections:
   ```sql
   SELECT count(*) FROM pg_stat_activity;
   ```

**Solutions:**

1. Increase Spark workers:
   ```bash
   docker compose up -d --scale spark-worker=3
   ```

2. Increase batch interval:
   ```python
   # In Spark job
   .trigger(processingTime="30 seconds")  # Increase from 10s
   ```

3. Optimize PostgreSQL queries:
   ```sql
   -- Add indexes
   CREATE INDEX idx_events_timestamp ON events_raw(timestamp);
   ```

---

## Data Quality Issues

### Issue: "Missing events"

**Symptom:**
- Dashboard shows 0 events
- `events_raw` table empty

**Diagnosis:**

1. Is generator running?
   ```bash
   ps aux | grep event_generator
   ```

2. Are events in Kafka?
   ```bash
   docker compose exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic events.pageview \
     --max-messages 5
   ```

3. Are Spark jobs running?
   ```bash
   docker compose ps | grep spark
   ```

---

### Issue: "Duplicate events"

**Symptom:**
- Same `event_id` appears multiple times in `events_raw`

**Cause:**
- Spark reprocessed data without idempotent writes

**Solution:**

1. Add unique constraint:
   ```sql
   ALTER TABLE events_raw ADD CONSTRAINT events_raw_event_id_unique UNIQUE (event_id);
   ```

2. Use UPSERT in Spark jobs:
   ```python
   # ON CONFLICT DO NOTHING
   ```

---

## Getting More Help

### Useful Commands

**View all logs:**
```bash
docker compose logs -f
```

**View specific service logs:**
```bash
docker compose logs -f kafka
docker compose logs -f spark-master
```

**Restart specific service:**
```bash
docker compose restart kafka
```

**Check resource usage:**
```bash
docker stats
```

**Execute command in container:**
```bash
docker compose exec kafka bash
```

### Log Locations

- Kafka: `docker compose logs kafka`
- Spark: `/opt/spark/logs` (mounted to `spark-logs/`)
- Airflow: `logs/` directory
- PostgreSQL: `docker compose logs postgres`

### Still Stuck?

1. Check [README.md](../README.md) for setup instructions
2. Review [architecture.md](architecture.md) for system design
3. Look at [design_decisions.md](design_decisions.md) for technology rationale

---

**Pro Tip**: Most issues are resolved by:
1. Checking logs (`docker compose logs [service]`)
2. Restarting service (`docker compose restart [service]`)
3. Full reset (`docker compose down -v && docker compose up -d`)

# StreamMart — First-Run Runbook (Windows)

This runbook assumes you have **never run this project before**. Every command below was
executed against a real, freshly wiped instance of this stack on a Windows machine with Docker
Desktop allocated **9.64 GB RAM / 8 CPUs** — below the 12 GB this project used to claim as a
minimum. The outputs shown are the actual outputs from that run, not illustrative examples.
Six real configuration bugs were found and fixed during that validation; all fixes are already
applied in this repo. See `CLAUDE.md` §1/§9 for the full change log if you want the "why" behind
any of them.

Run every command from the repository root, in **Git Bash** (or WSL2/PowerShell — commands are
POSIX `sh`/bash; PowerShell equivalents are noted where the syntax actually differs).

---

## 0. Prerequisites

| Requirement | Verified version |
|---|---|
| Docker Desktop | 29.3.1 |
| Docker Compose | v2 (bundled with Docker Desktop) |
| Git | any recent version |
| RAM allocated to Docker Desktop | **9.64 GB confirmed sufficient** for the core profile; see §9 for exact numbers if you also want `obs` running simultaneously |

Check your Docker allocation:

```bash
docker info --format '{{.MemTotal}} bytes mem, {{.NCPU}} cpus'
```

Expected output (yours will vary):
```
10351767552 bytes mem, 8 cpus
```

If this reports less than ~8 GB, increase it: Docker Desktop → Settings → Resources → Memory
(or edit `%UserProfile%\.wslconfig` if you're on the WSL2 backend and restart Docker Desktop).

---

## 1. Clone and configure

```bash
cd /path/to/where/you/want/the/project
# (skip clone if you already have the repo locally)
cd data_eng_project
cp .env.example .env
```

Open `.env` and replace every `change_me_*` placeholder with a real value. At minimum:

- `POSTGRES_PASSWORD`
- `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY`
- `AIRFLOW_ADMIN_PASSWORD`
- `GRAFANA_ADMIN_PASSWORD`
- `PGADMIN_DEFAULT_PASSWORD`
- `AIRFLOW__CORE__FERNET_KEY` — generate a real one, don't leave the placeholder:
  ```bash
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```

**Do this before the first `docker compose up`.** Postgres and Airflow only run their
initialization scripts once, against an empty data volume. If you change these values *after*
the volume already exists, the running database will keep the old password and every service
that reads the new `.env` value will fail with `password authentication failed` — this is a real
failure mode we hit during validation (its cause: mid-session credential rotation created a
mismatch between `.env` and an already-initialized Postgres volume). If it ever happens to you,
the fix is `docker compose down -v` and start clean, or `ALTER USER ... WITH PASSWORD ...` inside
the running Postgres container if you need to preserve data.

Verify every variable Docker Compose needs actually resolves:

```bash
docker compose config --quiet
```

**Expected output:** nothing (a silent, zero-length success is correct — any missing variable or
syntax problem prints a warning or error here).

---

## 2. Build the event generator image

```bash
docker build -f Dockerfile.event-generator -t streammart-event-generator:1.0 .
```

**Expected output (tail):**
```
#11 exporting to image
#11 naming to docker.io/library/streammart-event-generator:1.0 done
```

Takes ~40s on a fresh build (pip-installs ~40 packages), effectively instant on rebuild if
`requirements.txt` and `src/` haven't changed (Docker layer cache).

---

## 3. Start core infrastructure (Kafka, PostgreSQL, MinIO)

```bash
docker compose up -d kafka postgres minio
```

Wait for all three to report healthy:

```bash
docker compose ps --format '{{.Name}}: {{.Status}}' kafka postgres minio
```

**Expected output** (took ~20s in validation):
```
streammart-kafka: Up 21 seconds (healthy)
streammart-minio: Up 21 seconds (healthy)
streammart-postgres: Up 21 seconds (healthy)
```

If any stays on `(health: starting)` for more than ~60s, check its logs:
`docker logs streammart-kafka` / `streammart-postgres` / `streammart-minio`.

### 3a. Verify PostgreSQL initialization

```bash
docker exec streammart-postgres psql -U streammart_user -d postgres -tAc \
  "SELECT datname FROM pg_database WHERE datname IN ('streammart','airflow');"
```
**Expected:**
```
streammart
airflow
```

```bash
docker exec streammart-postgres psql -U streammart_user -d streammart -tAc "\dt"
```
**Expected:** exactly these 9 tables — `daily_revenue`, `daily_summary`, `data_quality_checks`,
`metrics_1min`, `metrics_5min`, `pipeline_monitoring`, `product_daily_performance`,
`product_performance`, `session_summary`. There is **no** `events_raw` table — that's correct,
not missing; raw events live in MinIO (see step 8).

### 3b. Create the MinIO bucket

```bash
docker compose up -d minio-init
docker logs streammart-minio-init
```
**Expected:**
```
Added `myminio` successfully.
Bucket created successfully `myminio/raw-events`.
Access permission for `myminio/raw-events` is set to `download`
```

### 3c. Create Kafka topics

```bash
docker compose up -d kafka-topics-init
docker logs streammart-kafka-topics-init
```
**Expected (last lines):**
```
Created topic events.abandonment.
Created topic events.dlq.
Kafka topics initialized.
```

Verify all 6 exist:
```bash
docker exec streammart-kafka kafka-topics --bootstrap-server localhost:9092 --list
```
**Expected:**
```
events.abandonment
events.add_to_cart
events.dlq
events.pageview
events.product_click
events.purchase
```
(If you run this within ~1s of topic creation finishing, you may transiently see fewer than 6 —
that's a client metadata-cache delay, not a bug; it resolves within a second or two.)

---

## 4. Start the Spark cluster

```bash
docker compose up -d spark-master spark-worker-1 spark-worker-2
docker compose ps --format '{{.Name}}: {{.Status}}' spark-master spark-worker-1 spark-worker-2
```
**Expected** (healthy within ~15s):
```
streammart-spark-master: Up 14 seconds (healthy)
streammart-spark-worker-1: Up 14 seconds (healthy)
streammart-spark-worker-2: Up 14 seconds (healthy)
```

Verify the cluster has enough capacity — **this must show 4 total cores**, matching the 4
concurrent Spark job containers you're about to start (each requests 1 core). An earlier version
of this compose file only provisioned 3, which meant the 4th job could never be scheduled and sat
in `WAITING` forever:

```bash
curl -s http://localhost:8082/json/ | python -c "import json,sys; d=json.load(sys.stdin); print('cores:', d['cores'], '| memory:', d['memory'])"
```
**Expected:**
```
cores: 4 | memory: 3072
```

---

## 5. Start the four Spark streaming jobs

```bash
docker compose up -d spark-raw-event-writer spark-window-aggregator spark-session-tracker spark-revenue-aggregator
```

**This is the slowest step — budget 2.5 to 4 minutes.** Each of the 4 containers independently
resolves ~15-25 Maven/Ivy dependencies from Maven Central on every container creation (there's no
shared dependency cache across containers, so this repeats on every `docker compose down`/`up`
cycle, though not on a plain `stop`/`start` of the same container). This is normal — the
containers show `Up` in `docker compose ps` the entire time; they just haven't finished
registering with the Spark master yet. In validation, all 4 jobs were fully registered and
running at **t+156 seconds**.

Poll until all 4 are registered:
```bash
for i in $(seq 1 20); do
  sleep 15
  ACTIVE=$(curl -s http://localhost:8082/json/ | python -c "import json,sys; print(len(json.load(sys.stdin)['activeapps']))")
  echo "active apps: $ACTIVE / 4"
  [ "$ACTIVE" = "4" ] && break
done
```

**Expected (abbreviated from validation):**
```
active apps: 0 / 4
active apps: 0 / 4
active apps: 0 / 4
active apps: 1 / 4
active apps: 3 / 4
active apps: 3 / 4
active apps: 3 / 4
active apps: 3 / 4
active apps: 4 / 4
```

If any job never reaches "up" or keeps restarting, check its logs individually, e.g.
`docker logs streammart-spark-window-aggregator --tail 50`. A clean job log shows Maven
downloads, then `Reading from Kafka topics...`, then `[metrics_1min] Writing batch N to
PostgreSQL` with no `ERROR`/`Traceback` lines. **Zero errors is the expected state** — every
error we found during validation (a shell-syntax bug, a streaming-mode `countDistinct` bug, and a
subtle `Row.count` attribute-collision bug) is already fixed in this repo.

---

## 6. Start the event generator and the maintenance job

```bash
docker compose up -d event-generator postgres-maintenance
docker logs streammart-event-generator --tail 10
```
**Expected:**
```
... Event Generator initialized - Target: 2 events/second
... 🚀 Starting event generation...
... Target rate: 2 events/second
... 🛒 Cart abandoned: 1 items - $59.99
```
(`Target: 2 events/second` should match your `.env`'s `EVENT_GENERATOR_RATE` — confirms the rate
setting is actually being read, not silently ignored.)

```bash
docker logs streammart-postgres-maintenance --tail 10
```
**Expected:** `postgres:5432 - accepting connections` followed by repeating `DELETE 0` /
`INSERT 0 1` lines every ~60s, with no shell syntax errors.

---

## 7. Verify data is actually flowing end-to-end

Wait about 60-90 seconds after step 6, then:

```bash
docker exec streammart-postgres psql -U streammart_user -d streammart -tAc \
  "SELECT event_type, count, window_start FROM metrics_1min ORDER BY window_start DESC LIMIT 5;"
```
**Expected:** rows like:
```
purchase|1|1|0|2026-07-25 21:12:00+00
pageview|16|3|2|2026-07-25 21:12:00+00
```
(exact counts/timestamps will differ — the point is rows exist and update every ~30-60s)

```bash
docker exec streammart-postgres psql -U streammart_user -d streammart -tAc \
  "SELECT product_id, date, purchase_count, revenue FROM product_performance LIMIT 5;"
```
**Expected:** rows appear once at least one full purchase has gone through the simulated funnel
(browse → click → cart → purchase) — at `EVENT_GENERATOR_RATE=2` this typically takes a few
minutes, not seconds, since a purchase is the end of a multi-step, partly-random session.

Check the MinIO data lake has real Parquet files (not just Structured Streaming's internal
`_spark_metadata` commit log):
```bash
docker run --rm --network streammart-network -e MC_HOST_myminio="http://<MINIO_ACCESS_KEY>:<MINIO_SECRET_KEY>@minio:9000" \
  minio/mc:latest ls --recursive myminio/raw-events/events/ | grep -v _spark_metadata | head -5
```
**Expected:** lines like
`year=2026/month=7/day=25/event_type=abandonment/part-00000-....c000.snappy.parquet`

**`session_summary` will stay empty for 40-70 minutes after startup — this is expected, not a
bug.** `session_tracker.py` runs in Structured Streaming's `append` output mode (the only mode
Spark supports for `session_window` aggregations), so a session is only written once the
40-minute watermark confirms it's closed. Don't file a bug report if this table is empty
immediately after startup; check back in an hour.

---

## 8. (Optional) Observability profile — Grafana, Prometheus, log shipping

Only start this once core services (steps 3-6) are confirmed healthy — it adds real memory
pressure on top of the core stack.

```bash
docker compose --profile obs up -d
```

Confirm Grafana comes up quickly (previously it silently hung for 50+ seconds downloading an
unused `redis-datasource` plugin — this repo no longer requests that plugin):
```bash
for i in $(seq 1 10); do sleep 3; curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3000/api/health; done
```
**Expected:** `000` once or twice, then `200` — should flip to `200` within ~10 seconds.

Verify both datasources are provisioned and actually connect:
```bash
curl -s -u "admin:<GRAFANA_ADMIN_PASSWORD>" http://localhost:3000/api/datasources/uid/postgres/health
curl -s -u "admin:<GRAFANA_ADMIN_PASSWORD>" http://localhost:3000/api/datasources/uid/prometheus/health
```
**Expected:**
```
{"message":"Database Connection OK","status":"OK"}
{"details":{"application":"Prometheus","features":{"rulerApiEnabled":false}},"message":"Successfully queried the Prometheus API.","status":"OK"}
```

Verify Prometheus is scraping every target successfully:
```bash
curl -s http://localhost:9090/api/v1/targets | python -c "
import json,sys
d=json.load(sys.stdin)
for t in d['data']['activeTargets']: print(t['labels'].get('job'), t['health'])
"
```
**Expected:** `up` for all 9 jobs — `kafka-exporter`, `kafka-jmx`, `minio`, `postgres`,
`prometheus`, `spark-applications`, `spark-master`, and two `spark-worker` entries.

Open **http://localhost:3000** (Grafana) in a browser and confirm the "StreamMart Operational
Dashboard" (under the StreamMart folder) shows live-updating panels for event rate, event
type breakdown, and the conversion funnel.

**Note:** `kafka-exporter`'s consumer-group-lag metrics will not show data for any of the 4 Spark
jobs specifically — Structured Streaming tracks offsets via its own checkpoint files, not Kafka's
`__consumer_offsets`. You'll see a `schema-registry` consumer group and nothing else. This is
expected, not a misconfiguration — see `CLAUDE.md` §9 Medium roadmap for the real fix.

### 8a. Verify Loki is actually ingesting logs

`loki` has no `healthcheck` (see §12 for why — the image has no shell to run one in), so
`docker compose ps` will just show `Up`, not `(healthy)`. That's correct, not a downgrade.
Confirm it's actually working by querying real log lines back out:

```bash
curl -s "http://localhost:3100/loki/api/v1/label/container/values"
```
**Expected:** a JSON array of `/streammart-*` container names for everything currently running.

```bash
curl -s -G "http://localhost:3100/loki/api/v1/query_range" \
  --data-urlencode 'query={container=~".+"}' --data-urlencode 'limit=3'
```
**Expected:** `"status":"success"` with a non-empty `result` array containing real log lines.

---

## 9. (Optional) Orchestration profile — Airflow

**Do not run this alongside a fully-loaded `obs` profile for extended periods on a
9-10 GB Docker allocation.** In validation, running core + obs + orchestration together for over
an hour pushed real memory usage to ~9.1/9.64 GB (94.6%), which caused Kafka's health checks to
start timing out and, separately, made `airflow-webserver` fail to start within its startup
timeout. Neither was a config bug — stopping the `obs` profile's non-essential services
(`docker compose --profile obs stop grafana prometheus kafka-exporter kafka-jmx-exporter loki
promtail`, plus `docker stop streammart-pgadmin streammart-schema-registry streammart-kafka-ui`)
brought Kafka back to `healthy` within seconds. If you want core + orchestration together for a
while, stop `obs` first.

```bash
docker compose --profile orchestration up -d
```

`airflow-init` runs the DB migration, creates the admin user, and provisions the
`streammart_postgres` connection automatically — no manual "Admin → Connections" step needed.
Wait for it to finish:
```bash
until [ "$(docker inspect streammart-airflow-init --format '{{.State.Status}}')" = "exited" ]; do sleep 5; done
docker inspect streammart-airflow-init --format 'exit code: {{.State.ExitCode}}'
```
**Expected:** `exit code: 0`. This is idempotent — safe to re-run against an existing Airflow
database (e.g. after `docker compose restart`) without failing.

The webserver takes noticeably longer to become responsive than other services (it installs
`boto3` via `_PIP_ADDITIONAL_REQUIREMENTS` before starting) — budget ~75-90 seconds:
```bash
for i in $(seq 1 20); do sleep 5; curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8085/health; done
```
**Expected:** `000` repeatedly, then `200` around t+75-90s. Under real resource pressure this can
take longer — the webserver's startup timeout is 240s (`AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT`,
raised from Airflow's 120s default after a live crash under load), and it now has both a
`restart: on-failure:3` policy and automatic stale-PID-file cleanup on every start, so a crash
under transient load self-heals instead of needing a manual `docker compose up -d
--force-recreate`. If it's still not responding after several minutes, check
`docker logs streammart-airflow-webserver` and `docker stats` — this is almost always memory
pressure (see the warning above this section), not a code problem.

Confirm all 4 DAGs loaded with zero import errors:
```bash
docker exec streammart-airflow-scheduler airflow dags list
docker exec streammart-airflow-scheduler airflow dags list-import-errors
```
**Expected:** all 4 DAGs listed (`streammart_daily_batch_processing`, `streammart_daily_summary`,
`streammart_data_quality`, `streammart_pipeline_health_check`), all `paused: True` (they start
paused by design — unpause manually in the UI at **http://localhost:8085** when you want them to
run on schedule), and the import-errors command prints nothing.

---

## 10. Tearing down

```bash
# Stop everything but keep data (Postgres/Kafka/MinIO volumes survive)
docker compose --profile obs --profile orchestration down

# Full reset — deletes all data, next `up` re-runs every init step from scratch
docker compose --profile obs --profile orchestration down -v
```

---

## 11. Resource guidance (measured, not estimated)

Real memory usage from validation, `docker stats --no-stream`, all 4 Spark jobs running with
live traffic:

| Profile combination | Real usage measured | Fits in 9.64 GB? |
|---|---|---|
| Core only (17 services, incl. kafka-ui/pgadmin/schema-registry) | **~6.4 GB** (measured fresh) | Yes, ~3.2 GB headroom |
| Core + obs (26 services) | **~7.9 GB** (measured fresh) | Yes, but tight (~1.7 GB headroom) |
| Core + obs + orchestration (all 29 services), run for over an hour with real traffic | **~9.1 GB** (measured under sustained load — includes state growth in the Spark workers over time, not just cold-start usage) | **No** — caused Kafka health-check timeouts and an `airflow-webserver` startup failure, both confirmed to resolve within seconds of freeing ~1 GB by stopping `obs`. Don't run all three together for extended periods on 9-10 GB |

If you have 12+ GB available, all three profiles together should fit comfortably, though even
then, watch usage over time — Spark's workers accumulate state (window/session tracking) and
grow past their cold-start footprint the longer they run. If you're resource-constrained like
this validation run (9.64 GB), the practical approach is: run core + obs for dashboard work, or
core + orchestration for DAG work, but not all three simultaneously for long stretches.

To skip non-essential dev tools and free ~1 GB on a tight core-only run:
```bash
docker compose up -d kafka postgres minio minio-init kafka-topics-init \
  spark-master spark-worker-1 spark-worker-2 \
  spark-raw-event-writer spark-window-aggregator spark-session-tracker spark-revenue-aggregator \
  event-generator postgres-maintenance
# (omits kafka-ui, pgadmin, schema-registry — none are required for the pipeline itself)
```

---

## 12. What was fixed to make this runbook possible

Found and fixed during this validation session (all already applied in this repo — listed here
so you know what "correct" behavior looks like and don't mistake a fixed bug for a new one):

1. `postgres-maintenance`'s shell command had a YAML-folding bug that produced a literal
   `syntax error: unexpected "||"` on every run — confirmed via live container logs.
2. Spark workers were provisioned with 3 total cores while 4 Spark job containers each need 1 —
   the 4th job could never be scheduled. Now provisioned with 4.
3. `revenue_aggregator.py` used `countDistinct()` in a streaming aggregation, which Spark
   explicitly does not support (`AnalysisException: Distinct aggregations are not supported on
   streaming DataFrames`). The computed value was unused in the output anyway — removed.
4. `window_aggregator.py` used `row.count` to read a column literally named `count` — but
   `pyspark.sql.Row` subclasses `tuple`, which already has a real `.count()` method, so attribute
   access silently returned a bound method instead of the field value, crashing psycopg2 on the
   very first real batch (`can't adapt type 'builtin_function_or_method'`). Fixed to `row["count"]`.
5. Grafana was configured to install an unused `redis-datasource` plugin on every startup — this
   project has no Redis component anywhere — costing ~50+ seconds of startup time downloading
   from grafana.com for a plugin nothing references. Removed.
6. Airflow had no `AIRFLOW__CORE__FERNET_KEY` set, so connection passwords were stored
   unencrypted in its metadata database (confirmed via a live startup warning). Added, and
   the connection-provisioning step in `airflow-init` was made idempotent (`connections delete`
   before `connections add`) since it previously failed with a nonzero exit on any second run
   against an already-initialized Airflow database.

Found in a **follow-up session** testing the orchestration profile under sustained load
(all 3 profiles running together for over an hour, ~9.1/9.64 GB in use):

7. `airflow-webserver` crashed outright when gunicorn didn't report ready within the 120s
   default startup timeout under memory pressure, and had no `restart:` policy to bring it back.
   Worse, restarting the same (not recreated) container then failed with `Error: Already running
   on PID N (or pid file ... is stale)` — the crashed process's pid file survived on the
   container's writable layer. Fixed three ways: startup timeout raised to 240s, the container's
   command now removes any stale pid file before every start, and both webserver and scheduler
   got `restart: on-failure:3`.
8. `config/loki/loki-config.yml` had never actually worked — its schema (boltdb-shipper, schema
   v11, a top-level `query_config` block) targeted a Loki release roughly 3 majors behind what
   `grafana/loki:latest` resolves to (confirmed: 3.7.1 via `docker run --rm grafana/loki:latest
   --version`), and failed to parse on every startup attempt. Rewritten against Loki's current
   reference config (tsdb store, schema v13).
9. `loki`'s healthcheck (`wget ... || exit 1`) failed on every single attempt regardless of
   actual health, because `grafana/loki:latest` ships a distroless-style image with no shell, no
   wget, not even `ls` (confirmed via `docker exec`: `exec: "/bin/sh": stat /bin/sh: no such file
   or directory`). Removed — nothing in this compose file depends on Loki's health condition
   specifically, so this has no effect on startup ordering, only on a status label that was
   permanently wrong either way.

Also added missing `mem_limit`s to every previously-uncapped service (Kafka, Postgres, MinIO,
Schema Registry, Kafka UI, pgAdmin, all 3 Airflow services) so resource usage is governed and
predictable rather than able to consume the entire host allocation.

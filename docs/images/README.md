# Screenshots & Demo — What to Capture

The main [README.md](../../README.md) reserves space for these but doesn't embed them yet — this
project would rather show an honest placeholder than a screenshot staged to look better than the
real system. If you're adding these, here's exactly what's expected and where it goes.

**Already done:** `architecture-diagram.svg` — a real, hand-built technical diagram, embedded in
the README already. Not a placeholder.

**Not yet done — real screenshots of the running system:**

| File | Capture | How |
|---|---|---|
| `dashboard-overview.png` | The Grafana "StreamMart Operational Dashboard" (9 panels) with real traffic flowing | `docker compose --profile obs up -d`, let the event generator run a few minutes, screenshot http://localhost:3000 |
| `airflow-dags.png` | The Airflow DAGs list view, all 4 DAGs visible | `docker compose --profile orchestration up -d`, screenshot http://localhost:8085 after `airflow-init` completes |
| `kafka-ui.png` | Kafka UI showing the 6 topics with live message throughput | http://localhost:8080 |
| `minio-console.png` | MinIO console showing the `raw-events` bucket with partitioned Parquet files | http://localhost:9001 |
| `demo.gif` | A ~20-30s terminal recording of `docker compose up -d` through to the first row landing in `metrics_1min` | [asciinema](https://asciinema.org/) + [agg](https://github.com/asciinema/agg) to convert to GIF, or [VHS](https://github.com/charmbracelet/vhs) |

**Optional — AI-generated hero banner:** see
[`ai-hero-banner-prompt.md`](ai-hero-banner-prompt.md) for a ready-to-use prompt for an abstract,
stylized cover image (not a technical diagram — that's what the SVG above already is). Generate
it externally, save as `hero-banner.png`, and follow the embed instructions at the bottom of that
file.

Once a file exists here, uncomment the corresponding `![...]` line in `README.md` (search for
`docs/images/` — the exact markdown is left commented out right where each image belongs).

Keep these current: if `ARCHITECTURE.md`'s Mermaid diagram or the dashboard panels change, the
screenshots should be retaken, not left stale. A screenshot that no longer matches the running
system is worse than no screenshot.

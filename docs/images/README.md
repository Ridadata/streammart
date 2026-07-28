# Screenshots & Demo — What to Capture

The main [README.md](../../README.md) reserves space for these but doesn't embed them yet — this
project would rather show an honest placeholder than a screenshot staged to look better than the
real system. If you're adding these, here's exactly what's expected and where it goes.

**Already done:**
- `architecture-diagram.svg` — a hand-built technical diagram. A follow-up prompt to regenerate
  it as a logo-accurate AI image is in [`ai-architecture-diagram-prompt.md`](ai-architecture-diagram-prompt.md)
  (the current SVG reads as visually flat; not yet replaced).
- `hero-banner.png` — an AI-generated abstract cover image, embedded under the badge row.
  Generated from [`ai-hero-banner-prompt.md`](ai-hero-banner-prompt.md).
- `dashboard-overview.png`, `airflow-dags.png`, `minio-console.png` — real screenshots of the
  running system, embedded in the README.

**Not yet done:**

| File | Capture | How |
|---|---|---|
| `kafka-ui.png` | Kafka UI showing the 6 topics with live message throughput | http://localhost:8080 |
| `demo.gif` | A ~20-30s terminal recording of `docker compose up -d` through to the first row landing in `metrics_1min` | [asciinema](https://asciinema.org/) + [agg](https://github.com/asciinema/agg) to convert to GIF, or [VHS](https://github.com/charmbracelet/vhs) |

Once a file exists here, uncomment the corresponding `![...]` line in `README.md` (search for
`docs/images/` — the exact markdown is left commented out right where each image belongs).

Keep these current: if `ARCHITECTURE.md`'s Mermaid diagram or the dashboard panels change, the
screenshots should be retaken, not left stale. A screenshot that no longer matches the running
system is worse than no screenshot.

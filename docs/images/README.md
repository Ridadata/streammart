# Screenshots & Demo — What to Capture

The main [README.md](../../README.md) reserves space for these but doesn't embed them yet — this
project would rather show an honest placeholder than a screenshot staged to look better than the
real system. If you're adding these, here's exactly what's expected and where it goes.

**Already done:**
- `architecture-diagram.png` — an AI-generated, logo-accurate technical diagram (replaced an
  earlier hand-built SVG that read as visually flat), generated from
  [`ai-architecture-diagram-prompt.md`](ai-architecture-diagram-prompt.md).
- `hero-banner.png` — an AI-generated abstract cover image, embedded under the badge row.
  Generated from [`ai-hero-banner-prompt.md`](ai-hero-banner-prompt.md).
- `dashboard-overview.png`, `airflow-dags.png`, `minio-console.png` — real screenshots of the
  running system, embedded in the README.
- `Streamart.gif` — a screen recording of the stack running end-to-end, embedded right under the
  hero banner at the top of the README.

**Not yet done:**

| File | Capture | How |
|---|---|---|
| `kafka-ui.png` | Kafka UI showing the 6 topics with live message throughput | http://localhost:8080 |

Keep these current: if `ARCHITECTURE.md`'s Mermaid diagram or the dashboard panels change, the
screenshots should be retaken, not left stale. A screenshot that no longer matches the running
system is worse than no screenshot.

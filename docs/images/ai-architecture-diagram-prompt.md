# AI Image Generation Prompt — Technical Architecture Diagram

This is the prompt used to generate `docs/images/architecture-diagram.png`, which is already
embedded in `README.md` (it replaced an earlier hand-built SVG that read as visually flat).
Kept here so the diagram can be regenerated or iterated on later — a polished,
professional-looking architecture diagram in the style used by real engineering blogs (Netflix
Tech Blog, Uber Engineering, Airbnb Engineering, the AWS Architecture Icon library) —
recognizable technology logos, clear left-to-right data flow, labeled arrows describing what
actually moves between each hop. Generate with GPT Image, Midjourney (v6+), or similar, then
drop the result at `docs/images/architecture-diagram.png` (same embed location in `README.md` —
see bottom of this file).

## The prompt

> A clean, professional software architecture diagram for a real-time data engineering pipeline,
> in the style of a top-tier engineering blog (Netflix/Uber/Airbnb tech blog, or an AWS
> reference-architecture diagram). Horizontal left-to-right data flow across six stages, each
> stage a distinct labeled box/card connected by directional arrows:
>
> **Stage 1 — Event Simulator**: a small icon representing a Python script/service generating
> e-commerce clickstream events (labeled "Event Simulator (Python)").
>
> **Stage 2 — Kafka**: a box containing the real Apache Kafka logo, labeled "Apache Kafka
> (KRaft)", showing 6 small topic pills inside it (pageview, product_click, add_to_cart,
> purchase, abandonment, dlq).
>
> **Stage 3 — Spark**: a box containing the real Apache Spark logo, labeled "Spark Structured
> Streaming", branching into 4 parallel labeled sub-processes (Raw Event Writer, Window
> Aggregator, Session Tracker, Revenue Aggregator) shown as small parallel lanes fanning out then
> reconverging.
>
> **Stage 4 — Storage**: two side-by-side boxes — one with the real MinIO logo labeled "MinIO
> (S3-compatible data lake)", one with the real PostgreSQL elephant logo labeled "PostgreSQL
> (serving layer)" containing small nested table icons.
>
> **Stage 5 — Orchestration**: a box with the real Apache Airflow logo, labeled "Airflow (batch
> reconciliation)", with an arrow looping back into the PostgreSQL box to show nightly batch
> writes.
>
> **Stage 6 — Observability & Dashboards**: two boxes on a lower parallel row — one with the real
> Prometheus logo (torch icon) labeled "Prometheus", one with the real Grafana logo labeled
> "Grafana", both connected with dashed "scrapes"/"queries" arrows back up into the Spark and
> PostgreSQL boxes (not the main left-to-right flow, a secondary monitoring layer underneath).
>
> Every arrow between stages is labeled with what actually flows across it in small caption text
> (e.g. "JSON events", "Parquet files", "SQL upsert", "scheduled DAG run"). Use authentic,
> recognizable technology logos for Kafka, Spark, PostgreSQL, MinIO, Airflow, Prometheus, and
> Grafana — not generic placeholder icons. Layout: clean swimlane/flowchart style, generous
> whitespace, rounded rectangle nodes, consistent icon sizing, subtle drop shadows, a light
> background (#ffffff or #f6f8fa, GitHub-README-friendly) with dark, legible text (#1f2328), and
> each technology's own real brand color used only as an accent on its box border/icon (Kafka
> black, Spark orange #E25A1C, PostgreSQL blue #336791, MinIO red #C4362D, Airflow teal #017CEE,
> Prometheus orange #E6522C, Grafana orange #F46800). Sans-serif, modern typeface for all labels.
> Wide aspect ratio suitable for a GitHub README, roughly 1600×900 (16:9), high resolution, crisp
> vector-like rendering, no photorealism. Negative prompt / avoid: hand-drawn or sketchy style,
> cartoonish mascots, generic unlabeled boxes, illegible or decorative-only text, cluttered
> overlapping elements, dark-mode-only background, watermarks, any fictional or incorrect logos.

## Why this framing

- **Real logos, not abstractions** — this is the opposite brief from `ai-hero-banner-prompt.md`
  (that one is deliberately abstract/text-free). Here the whole point is instant technology
  recognition: a reviewer should identify Kafka, Spark, Postgres, Airflow, Grafana, and
  Prometheus at a glance, the way they would in a real engineering-blog diagram.
- **Labeled arrows, not just labeled boxes** — matches how this project's own `ARCHITECTURE.md`
  describes the pipeline (JSON events → Parquet / upserts / DAG runs), so the generated image
  stays consistent with the written documentation instead of just being decorative.
- **Light background** — GitHub renders README images on a white (light theme) or near-black
  (dark theme) canvas depending on the viewer's setting; a light-background diagram with dark
  text stays legible in both, whereas the previous SVG's dark boxes on transparent background
  didn't adapt to theme.
- **Six-stage swimlane structure** mirrors the actual data flow order documented in
  `ARCHITECTURE.md` and `CLAUDE.md` §2 exactly: simulator → Kafka → Spark (4 jobs) → MinIO +
  Postgres → Airflow → Prometheus/Grafana observability layer underneath.

## If you regenerate it

`README.md` already embeds it at `docs/images/architecture-diagram.png`, so a regenerated image
just needs to overwrite that file (same filename, same embed block — no README edit needed):

```markdown
<div align="center">

![StreamMart Architecture](docs/images/architecture-diagram.png)

<sub>Full detail: <a href="ARCHITECTURE.md">ARCHITECTURE.md</a></sub>

</div>
```

Keep the file under ~1MB. If the generated image has any inaccuracies (wrong logo, wrong flow
direction, a missing hop), regenerate rather than hand-editing — a raster image can't be patched
the way an SVG could.

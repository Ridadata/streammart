# AI Image Generation Prompt — Hero Banner

The SVG at `docs/images/architecture-diagram.svg` is the *technical* diagram — accurate, labeled,
kept in sync with the real pipeline. This is a prompt for a *stylized* hero banner instead: an
artistic cover image for the top of the README, the kind of abstract visual that communicates
"real-time data at scale" at a glance before anyone reads a word. Generate it with
Midjourney, DALL·E, Stable Diffusion, or similar, then drop the result at
`docs/images/hero-banner.png` (or `.jpg`) and add it near the top of `README.md`, right under the
badge row.

## The prompt

> A wide, cinematic banner illustration for a real-time data engineering platform. A dense
> network of glowing data streams — thin luminous lines in electric orange and deep blue — flows
> left to right across a dark navy background, converging from many scattered points on the left
> (representing thousands of e-commerce clickstream events: shopping carts, product tags, click
> cursors, rendered as small minimalist glowing icons) through a central cluster of geometric
> nodes (representing Kafka and Spark — hexagonal and circular nodes connected by bright pulsing
> lines, like a distributed systems diagram but abstract and atmospheric, not literal) and out
> into a calm, orderly grid of soft blue rectangles on the right (representing a database and
> dashboard, glowing gently, steady rather than chaotic — contrast between the turbulent left
> side and the settled right side is the whole point). Style: modern tech/SaaS editorial
> illustration, isometric-influenced but not strict isometric, dark mode aesthetic, high contrast,
> subtle bloom/glow on the light sources, clean vector-like shapes rather than photorealism, in
> the visual language of GitHub/Vercel/Linear marketing pages. Color palette: navy/charcoal
> background (#0d1117 range), electric orange accents (#E25A1C — Spark's actual brand color),
> cool blue accents (#1f3a5f / #336791 — Postgres blue), small warm-white highlights. No text, no
> logos, no readable labels anywhere in the image. Wide aspect ratio suitable for a GitHub README
> banner, roughly 1200×400 (3:1). Negative prompt / avoid: photorealistic people, literal screen
> UI mockups, generic "matrix code rain" cliché, cluttered chaos with no visual hierarchy, stock
> photo look, any readable text or numbers.

## Why this framing

- **Left → right motion** mirrors the pipeline's actual left-to-right data flow (simulator →
  Kafka → Spark → storage), so the image is thematically honest even though it's abstract, not
  a literal diagram.
- **Turbulent → orderly** visually encodes the real architectural idea this project is built
  around: raw, chaotic events get progressively organized into clean, queryable, single-writer
  tables. That's not decoration — it's the actual thesis of the pipeline, illustrated.
- **Real brand colors** (Spark orange, Postgres blue) tie the abstract art back to the concrete
  tech stack, so it doesn't feel disconnected from the rest of the README.
- **No text in the image** is deliberate: GitHub renders the README's actual heading and badges
  right above it, so a text-free banner won't visually compete or look outdated if copy changes
  later.

## Once you have it

```markdown
<div align="center">

![StreamMart](docs/images/hero-banner.png)

</div>
```

Place this block directly under the badge row (before the `[Quick Start](#-quick-start) · ...`
quick-links line) in `README.md`. Keep the file under ~500KB — a README hero doesn't need
print-resolution source files, and large images slow down the page for anyone on a mediocre
connection reviewing your repo from their phone.

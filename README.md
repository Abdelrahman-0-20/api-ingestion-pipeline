# API Ingestion Pipeline

Project #2 of a beginner data-engineering portfolio: a **live API → database**
pipeline. Ingests the **USGS earthquake feed** (public GeoJSON, no API key,
updates every minute) into **DuckDB** with retries, content-addressed raw
storage, incremental upserts by natural key, schema-drift alerts, and a
freshness SLA — all gated by **pytest** and scheduled by **GitHub Actions**.

## Architecture

```
USGS GeoJSON feed (live, no key)
        │  fetch.py — GET with retry/backoff, 4xx fails loud (no retry)
        ▼
data/raw/{sha256}.json        content-addressed raw files (immutable, deduped)
        │  ingest — one bronze row per *new* payload (hash-guarded)
        ▼
bronze_fetches                payload + fetched_at + source_url + sha256
        │  clean_features() — parse, type-check, ranges, domain values ← pytest
        ▼
silver_earthquakes            upsert by event id: new rows insert,
                              revised events replace, history preserved
        │  SQL aggregates
        ▼
gold_daily_seismic            events/day, max magnitude, strong-quakes count
        │
        ▼
freshness SLA gate            newest event must be < 48h old or the run fails
pipeline_log                  every run recorded
```

## Run it

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python -m src.pipeline   # fetch → bronze → silver → gold + freshness gate
python -m pytest -q      # data quality gates must pass
```

Run it twice: the second run reports `new: 0, changed: 0` — the proof that
upserts and dedupe actually work.

## How this differs from project #1 (file-based ETL)

| Concern | Project #1 (batch files) | This project (live API) |
|---|---|---|
| Ingestion | immutable file, fetched once | HTTP with retry/backoff, 4xx vs 5xx handling |
| Change detection | file already on disk | sha256 content hash of the payload |
| Load style | append-once bronze | **incremental upsert by natural key** (event id) |
| Data quality | static rules | rules + **schema-drift alerts** (missing fields reported) |
| Reliability | correctness on rerun | correctness on rerun + **freshness SLA gate** |

## Design decisions (interview talking points)

| Decision | Why |
|---|---|
| Retry with backoff, but never on 4xx | client errors are deterministic — retrying just delays the real failure |
| Content-addressed raw files (`{sha256}.json`) | unchanged data = same hash = no duplicate storage, natural change detection |
| Upsert by `id`, stage-wins-union | USGS revises events (magnitude updates); old versions must be replaced, not duplicated |
| Freshness SLA as a failing gate | stale data delivered silently is worse than a red build |
| Missing-field counts in every report | partial drift shows up as an alert before it becomes corruption |
| GeoJSON `[lon, lat, depth]` order asserted in tests | coordinate swaps are the classic geospatial bug — tests pin the order |

## Resume line

> Built a live API ingestion pipeline (Python/requests/DuckDB) with retry/backoff,
> content-addressed raw storage, incremental upserts by natural key, schema-drift
> detection, a freshness SLA gate, and scheduled runs via GitHub Actions.

## Ideas for later

- Streamlit map of events (GeoPandas / folium on the Gold layer)
- Alerting when freshness SLA fails (email/Slack webhook)
- Partition silver by event date as the feed grows

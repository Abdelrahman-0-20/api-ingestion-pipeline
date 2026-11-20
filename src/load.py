"""Bronze -> Silver -> Gold into DuckDB. Every step idempotent: rerun = same result."""
import json
from datetime import datetime, timezone

import duckdb

from .clean import clean_features
from .config import resolve


def connect(cfg: dict) -> duckdb.DuckDBPyConnection:
    db_path = resolve(cfg["paths"]["warehouse"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def ingest_bronze(cfg: dict, con, payload: dict, sha: str) -> int:
    """Append one fetch record to Bronze — skipped if this payload was already ingested."""
    url = cfg["source"]["feed_url"]
    source_system = cfg["source"]["source_system"]

    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze_fetches (
            fetched_at TIMESTAMP,
            source_url VARCHAR,
            source_system VARCHAR,
            payload_sha256 VARCHAR,
            feature_count BIGINT,
            payload VARCHAR
        )
    """)
    if con.execute("SELECT 1 FROM bronze_fetches WHERE payload_sha256 = ?",
                   [sha]).fetchone():
        print(f"[bronze] payload {sha[:12]} already ingested — skipped (idempotent)")
        return 0

    features = len(payload.get("features", []))
    con.execute("INSERT INTO bronze_fetches VALUES (?, ?, ?, ?, ?, ?)",
                [datetime.now(timezone.utc).replace(tzinfo=None), url,
                 source_system, sha, features, json.dumps(payload)])
    print(f"[bronze] appended fetch ({features:,} features)")
    return 1


def build_silver(cfg: dict, con) -> dict:
    """Parse the latest Bronze payload and upsert into Silver by event id.

    New/changed events replace old versions; untouched history is preserved.
    Rebuilding from the same payload changes nothing (idempotent).
    """
    # fetchall() fully consumes the result — DuckDB rejects register()
    # while a previous result is still open on the connection.
    rows = con.execute(
        "SELECT payload FROM bronze_fetches ORDER BY fetched_at DESC LIMIT 1"
    ).fetchall()
    if not rows:
        raise RuntimeError("Bronze is empty — fetch data first")

    payload = json.loads(rows[0][0])
    events, report = clean_features(payload, cfg["cleaning"])
    events["source_system"] = cfg["source"]["source_system"]
    events["ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None)

    con.register("stage_events", events)
    try:
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_name = 'silver_earthquakes'"
        ).fetchone()[0] > 0

        if not exists:
            con.execute("CREATE TABLE silver_earthquakes AS SELECT * FROM stage_events")
            report["new_events"] = report["output_rows"]
            report["changed_events"] = 0
        else:
            con.execute("CREATE OR REPLACE TABLE _silver_prev AS "
                        "SELECT * FROM silver_earthquakes")
            report["new_events"] = con.execute("""
                SELECT COUNT(*) FROM stage_events s
                LEFT JOIN _silver_prev p ON s.id = p.id WHERE p.id IS NULL
            """).fetchone()[0]
            report["changed_events"] = con.execute("""
                SELECT COUNT(*) FROM stage_events s
                JOIN _silver_prev p ON s.id = p.id
                WHERE s.mag IS DISTINCT FROM p.mag
                   OR s.place IS DISTINCT FROM p.place
                   OR s.event_time IS DISTINCT FROM p.event_time
                   OR s.depth_km IS DISTINCT FROM p.depth_km
            """).fetchone()[0]
            # Stage rows win for ids they contain; everything else is history.
            con.execute("""
                CREATE OR REPLACE TABLE silver_earthquakes AS
                SELECT * FROM stage_events
                UNION ALL
                SELECT * FROM _silver_prev
                WHERE id NOT IN (SELECT id FROM stage_events)
            """)
            con.execute("DROP TABLE _silver_prev")

        # fetchall() (not fetchone) — unregister() rejects open results.
        report["silver_total"] = con.execute(
            "SELECT COUNT(*) FROM silver_earthquakes").fetchall()[0][0]
    finally:
        con.unregister("stage_events")

    print(f"[silver] {report['input_rows']:,} in -> {report['output_rows']:,} valid "
          f"({report['rejected_total']:,} rejected) | new: {report['new_events']}, "
          f"changed: {report['changed_events']} | silver total: {report['silver_total']:,}")
    if report["missing_fields"]:
        print(f"[silver] alert — missing/null fields detected: {report['missing_fields']}")
    return report


def build_gold(con) -> None:
    """Daily seismic aggregates. CREATE OR REPLACE = rerun safe."""
    con.execute("""
        CREATE OR REPLACE TABLE gold_daily_seismic AS
        SELECT CAST(event_time AS DATE) AS event_date,
               COUNT(*) AS event_count,
               MAX(mag) AS max_magnitude,
               MIN(mag) AS min_magnitude,
               ROUND(AVG(depth_km), 2) AS avg_depth_km,
               SUM(CASE WHEN mag >= 4.5 THEN 1 ELSE 0 END) AS strong_quakes,
               CURRENT_TIMESTAMP AS refreshed_at
        FROM silver_earthquakes
        GROUP BY 1
        ORDER BY 1
    """)
    n = con.execute("SELECT COUNT(*) FROM gold_daily_seismic").fetchone()[0]
    print(f"[gold] gold_daily_seismic: {n} rows")


def check_freshness(cfg: dict, con) -> float:
    """SLA gate: newest event age must be under the configured threshold."""
    row = con.execute("SELECT MAX(event_time) FROM silver_earthquakes").fetchone()[0]
    if row is None:
        raise RuntimeError("Silver is empty — cannot check freshness")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    age_hours = (now - row).total_seconds() / 3600
    limit = float(cfg["freshness"]["max_age_hours"])
    if age_hours > limit:
        raise RuntimeError(
            f"Freshness SLA violated: newest event is {age_hours:.1f}h old "
            f"(limit {limit}h) — upstream feed may be stale")
    print(f"[freshness] newest event {age_hours:.1f}h old (SLA {limit:.0f}h) OK")
    return age_hours


def log_run(con, step_name: str, report: dict) -> None:
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_log (
            ran_at TIMESTAMP,
            step VARCHAR,
            input_rows BIGINT,
            output_rows BIGINT,
            rejected BIGINT,
            detail VARCHAR
        )
    """)
    detail = {k: v for k, v in report.items()
              if k not in ("input_rows", "output_rows", "rejected_total")}
    con.execute("INSERT INTO pipeline_log VALUES (now(), ?, ?, ?, ?, ?)", [
        step_name,
        report.get("input_rows"),
        report.get("output_rows"),
        report.get("rejected_total"),
        json.dumps(detail, default=str),
    ])

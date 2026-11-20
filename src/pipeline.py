"""Run the full pipeline: python -m src.pipeline"""
from . import load as etl
from .config import load_config
from .fetch import fetch_feed


def main() -> None:
    cfg = load_config()
    print("=== API ingestion pipeline ===")
    payload, sha, is_new = fetch_feed(cfg)

    con = etl.connect(cfg)
    try:
        etl.ingest_bronze(cfg, con, payload, sha)
        silver_report = etl.build_silver(cfg, con)
        etl.build_gold(con)
        etl.check_freshness(cfg, con)
        etl.log_run(con, "silver_upsert", silver_report)

        print("\nStrongest events in Silver:")
        for row in con.execute("""
            SELECT mag, place, event_time
            FROM silver_earthquakes ORDER BY mag DESC NULLS LAST LIMIT 5
        """).fetchall():
            print("  ", row)

        print("\nLast 5 days:")
        for row in con.execute("""
            SELECT event_date, event_count, max_magnitude, strong_quakes
            FROM gold_daily_seismic ORDER BY event_date DESC LIMIT 5
        """).fetchall():
            print("  ", row)
    finally:
        con.close()
    print("\nDone. Warehouse: warehouse/seismic.duckdb")


if __name__ == "__main__":
    main()

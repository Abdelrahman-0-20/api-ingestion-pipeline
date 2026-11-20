"""Prove bronze dedupe and silver upsert are idempotent."""
from datetime import datetime, timezone

import pytest

from src.fetch import payload_hash
from src import load as etl

NOW_MS = int(datetime.now(timezone.utc).timestamp() * 1000)

RULES = {
    "required_columns": ["id", "mag", "place", "event_time", "event_type",
                         "longitude", "latitude", "depth_km"],
    "accepted_event_types": ["earthquake"],
    "mag": {"min": -2.0, "max": 10.0},
    "depth_km": {"min": -100.0, "max": 700.0},
    "max_future_hours": 24,
    "max_age_days": 35,
}


def _payload(features):
    return {"type": "FeatureCollection", "features": features}


def _feature(id, mag):
    return {"id": id,
            "properties": {"mag": mag, "place": "Somewhere", "time": NOW_MS,
                           "type": "earthquake"},
            "geometry": {"coordinates": [10.0, 45.0, 8.0]}}


def _cfg(tmp_path):
    return {
        "source": {"feed_url": "https://example.invalid/feed",
                   "source_system": "test_feed"},
        "paths": {"warehouse": str(tmp_path / "seismic.duckdb"),
                  "raw_dir": str(tmp_path / "raw")},
        "cleaning": RULES,
        "freshness": {"max_age_hours": 48},
    }


def test_payload_hash_is_deterministic():
    a = _payload([_feature("x", 3.0)])
    b = _payload([_feature("x", 3.0)])
    c = _payload([_feature("x", 3.1)])
    assert payload_hash(a) == payload_hash(b)
    assert payload_hash(a) != payload_hash(c)


def test_bronze_and_silver_are_idempotent_with_updates(tmp_path):
    cfg = _cfg(tmp_path)
    con = etl.connect(cfg)
    try:
        payload_v1 = _payload([_feature("e1", 4.0), _feature("e2", 2.5)])
        sha_v1 = payload_hash(payload_v1)

        # First run: ingest + build
        assert etl.ingest_bronze(cfg, con, payload_v1, sha_v1) == 1
        r1 = etl.build_silver(cfg, con)
        assert r1["new_events"] == 2
        assert r1["silver_total"] == 2

        # Re-run same payload: bronze skips, silver unchanged
        assert etl.ingest_bronze(cfg, con, payload_v1, sha_v1) == 0
        r2 = etl.build_silver(cfg, con)
        assert r2["new_events"] == 0
        assert r2["changed_events"] == 0
        assert r2["silver_total"] == 2

        # Updated payload: e1 magnitude revised, e3 is new, e2 unchanged
        payload_v2 = _payload([_feature("e1", 4.6), _feature("e2", 2.5),
                               _feature("e3", 5.1)])
        sha_v2 = payload_hash(payload_v2)
        assert etl.ingest_bronze(cfg, con, payload_v2, sha_v2) == 1
        r3 = etl.build_silver(cfg, con)
        assert r3["new_events"] == 1          # e3
        assert r3["changed_events"] == 1      # e1 revised magnitude
        assert r3["silver_total"] == 3

        mag = con.execute(
            "SELECT mag FROM silver_earthquakes WHERE id = 'e1'").fetchone()[0]
        assert mag == 4.6                     # update replaced the old value
        assert con.execute("SELECT COUNT(*) FROM bronze_fetches").fetchone()[0] == 2
    finally:
        con.close()

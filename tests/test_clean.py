"""Prove the cleaner parses GeoJSON correctly and rejects bad events."""
from datetime import datetime, timezone

import pytest

from src.clean import clean_features

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


def _feature(id="abc123", mag=4.2, place="10 km NE of Somewhere",
             time=None, event_type="earthquake", coords=None):
    return {
        "id": id,
        "properties": {"mag": mag, "place": place,
                       "time": NOW_MS if time is None else time,
                       "type": event_type},
        "geometry": {"coordinates": [10.5, 45.5, 12.3] if coords is None else coords},
    }


def test_parses_geojson_with_correct_coordinate_order():
    # GeoJSON is [longitude, latitude, depth] — swapping them is the classic bug.
    out, report = clean_features({"features": [_feature()]}, RULES)
    row = out.iloc[0]
    assert row["longitude"] == 10.5
    assert row["latitude"] == 45.5
    assert row["depth_km"] == 12.3
    assert report["output_rows"] == 1


def test_rejects_bad_events_and_keeps_good_ones():
    future_ms = NOW_MS + 48 * 3600 * 1000
    out, report = clean_features({"features": [
        _feature(),                                             # good
        _feature(id="null_mag", mag=None),                      # null mag
        _feature(id="blast", event_type="quarry blast"),        # wrong domain
        _feature(id="huge", mag=15.0),                          # out of range
        _feature(id="future", time=future_ms),                  # in the future
        _feature(),                                              # exact duplicate of good row
    ]}, RULES)
    assert len(out) == 1
    assert report["rejected"]["null_values"] == 1
    assert report["rejected"]["event_type_in_domain"] == 1
    assert report["rejected"]["mag_in_range"] == 1
    assert report["rejected"]["event_time_not_future"] == 1
    assert report["rejected"]["duplicate_ids"] == 1


def test_schema_drift_fails_loud():
    with pytest.raises(ValueError, match="features"):
        clean_features({"metadata": {}}, RULES)


def test_empty_feed_fails_loud():
    with pytest.raises(ValueError, match="zero features"):
        clean_features({"features": []}, RULES)


def test_all_rejected_fails_loud():
    with pytest.raises(ValueError, match="rejected"):
        clean_features({"features": [_feature(id="only_bad", mag=None)]}, RULES)

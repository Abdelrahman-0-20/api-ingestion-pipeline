"""Silver cleaning rules — pure functions so pytest can prove them."""
from datetime import datetime, timezone

import pandas as pd


def _extract(feature: dict) -> dict:
    """GeoJSON feature -> flat row. Coordinates are [longitude, latitude, depth]."""
    props = feature.get("properties") or {}
    coords = (feature.get("geometry") or {}).get("coordinates") or []
    return {
        "id": feature.get("id"),
        "mag": props.get("mag"),
        "place": props.get("place"),
        "event_time": props.get("time"),       # epoch milliseconds (UTC)
        "event_type": props.get("type"),
        "longitude": coords[0] if len(coords) > 0 else None,
        "latitude": coords[1] if len(coords) > 1 else None,
        "depth_km": coords[2] if len(coords) > 2 else None,
    }


def clean_features(payload: dict, rules: dict) -> tuple[pd.DataFrame, dict]:
    if "features" not in payload:
        raise ValueError("Schema drift: payload has no 'features' key")
    if not payload["features"]:
        raise ValueError("Feed returned zero features — refusing to build Silver")

    df = pd.DataFrame(_extract(f) for f in payload["features"])
    # Missing/renamed upstream fields become nulls here and are counted below —
    # partial drift is reported, total drift trips the all-rows-rejected gate.
    missing = {c: int(df[c].isna().sum())
               for c in rules["required_columns"] if df[c].isna().any()}

    # Epoch ms -> naive UTC timestamps (unit conversion is UTC-based).
    df["event_time"] = pd.to_datetime(df["event_time"], unit="ms", errors="coerce")

    required = rules["required_columns"]
    rejected: dict[str, int] = {}

    def step(name: str, mask) -> None:
        nonlocal df
        before = len(df)
        df = df[mask(df)]
        rejected[name] = before - len(df)

    step("null_values", lambda d: d[required].notna().all(axis=1))

    accepted = rules["accepted_event_types"]
    step("event_type_in_domain", lambda d: d["event_type"].isin(accepted))

    for col in ("mag", "depth_km"):
        lo, hi = rules[col]["min"], rules[col]["max"]
        step(f"{col}_in_range", lambda d, lo=lo, hi=hi: (d[col] >= lo) & (d[col] <= hi))

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    future = now.replace(microsecond=0) + pd.Timedelta(hours=rules["max_future_hours"])
    stale = now.replace(microsecond=0) - pd.Timedelta(days=rules["max_age_days"])
    step("event_time_not_future", lambda d: d["event_time"] <= future)
    step("event_time_not_stale", lambda d: d["event_time"] >= stale)

    # One row per event id: the newest occurrence in this payload wins.
    step("duplicate_ids", lambda d: ~d.duplicated(subset=["id"]))

    report = {
        "input_rows": int(len(payload["features"])),
        "rejected": rejected,
        "rejected_total": int(sum(rejected.values())),
        "output_rows": int(len(df)),
        "missing_fields": missing,
    }
    if report["output_rows"] == 0:
        raise ValueError(f"All features rejected — refusing to write empty Silver: {rejected}")
    return df.reset_index(drop=True), report

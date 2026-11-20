"""HTTP ingestion with retries and content-addressed raw storage."""
import hashlib
import json
import time
import urllib.request
from pathlib import Path

import requests

from .config import resolve

USER_AGENT = "Mozilla/5.0 (portfolio-etl)"


def payload_hash(payload: dict) -> str:
    """Deterministic content hash — identical payloads always hash identically."""
    canonical = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _get_with_retries(url: str, http: dict) -> dict:
    retries = int(http["retries"])
    backoff = float(http["backoff_seconds"])
    timeout = float(http["timeout_seconds"])
    last_err: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout,
                                headers={"User-Agent": USER_AGENT})
            if 400 <= resp.status_code < 500:
                # Client error: retrying cannot help — fail loud immediately.
                raise RuntimeError(
                    f"Feed rejected the request (HTTP {resp.status_code}) — not retrying: {url}")
            resp.raise_for_status()          # 5xx / network: retried below
            payload = resp.json()
            if "features" not in payload:
                # Schema drift alert: upstream contract broken.
                raise ValueError("Schema drift: feed payload has no 'features' key")
            return payload
        except (requests.RequestException, ValueError, RuntimeError) as e:
            last_err = e
            if isinstance(e, RuntimeError) and "not retrying" in str(e):
                raise
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise RuntimeError(f"Feed fetch failed after {retries} attempts: {url}") from last_err


def fetch_feed(cfg: dict) -> tuple[dict, str, bool]:
    """Fetch the feed. Returns (payload, sha256, is_new_file).

    Raw files are content-addressed: identical payloads map to the same file,
    so re-fetching unchanged data never overwrites or duplicates anything.
    """
    url = cfg["source"]["feed_url"]
    payload = _get_with_retries(url, cfg["http"])
    sha = payload_hash(payload)

    raw_dir = resolve(cfg["paths"]["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f"{sha}.json"
    if raw_path.exists():
        print(f"[fetch] unchanged payload {sha[:12]} — raw file already stored")
        return payload, sha, False

    raw_path.write_text(json.dumps(payload), encoding="utf-8")
    n = len(payload.get("features", []))
    print(f"[fetch] new payload {sha[:12]} ({n:,} features)")
    return payload, sha, True

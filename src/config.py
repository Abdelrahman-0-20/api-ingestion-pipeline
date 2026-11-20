"""Load pipeline configuration. All paths and thresholds come from config.yaml."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


def load_config(path: Path | None = None) -> dict:
    path = path or ROOT / "config.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError(f"Config is empty or invalid: {path}")
    return cfg


def resolve(path_str: str) -> Path:
    """Resolve a possibly-relative config path against the project root."""
    p = Path(path_str)
    return p if p.is_absolute() else ROOT / p

from __future__ import annotations
from pathlib import Path
import yaml


def load_config() -> dict:
    root_dir = Path(__file__).resolve().parents[1]
    cfg_path = root_dir / "config.yaml"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Missing config.yaml at: {cfg_path}")
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
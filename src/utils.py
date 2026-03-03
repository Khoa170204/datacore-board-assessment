from __future__ import annotations
import logging
from datetime import datetime, timezone
import re


def setup_logger(name: str = "pipeline") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s"))
    logger.addHandler(handler)
    return logger


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


_ws = re.compile(r"\s+")


def clean_text(s: str | None) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = _ws.sub(" ", s)
    return s.strip()
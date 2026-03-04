import logging
import re
import random
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import yaml
from playwright.sync_api import TimeoutError as PlaywrightTimeout

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")
_ws = re.compile(r"\s+") #a compiled regular expression that matches one or more whitespace characters


# =====================================================
# LOGGER
# Configure and return a reusable logger instance.
# If the logger already exists, reuse it to avoid duplicate handlers.
# Logs are printed to stdout with timestamp and level formatting.
def setup_logger(name: str = "pipeline") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
    )
    logger.addHandler(handler)
    return logger


# =====================================================
# CONFIG LOADER
# Load application configuration from config.yaml.
# The file is expected at the project root level.
# Raises FileNotFoundError if the configuration file is missing.
def load_config() -> dict:
    root_dir = Path(__file__).resolve().parents[1]
    cfg_path = root_dir / "config.yaml"

    if not cfg_path.exists():
        raise FileNotFoundError(f"Missing config.yaml at: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# =====================================================
# HELPER
# Return the current timestamp in ISO format
# using Vietnam timezone (Asia -> Ho_Chi_Minh).
def now_vn_iso() -> str:
    return datetime.now(VN_TZ).isoformat()

# =====================================================
# TEXT CLEANING
# Normalize raw text by:
#   - Replacing non-breaking spaces
#   - Collapsing multiple whitespace characters
#   - Trimming leading and trailing spaces
# Returns empty string if input is None or empty.
def clean_text(s: str | None) -> str:
    if not s:
        return ""
    s = s.replace("\xa0", " ")
    s = _ws.sub(" ", s)
    return s.strip()

# =====================================================
# HTML FETCH WITH RETRIES
# Load a webpage using Playwright and return its HTML content.
# Implements retry logic to handle temporary timeouts.
# Adds a small random delay to reduce detection risk.
# Returns None if all attempts fail.
def fetch_html(page, url: str, retries: int = 2) -> str | None:
    for attempt in range(retries + 1):
        try:
            page.goto(url, timeout=45000, wait_until="domcontentloaded")
            page.wait_for_timeout(random.randint(1500, 3000))
            return page.content()
        except PlaywrightTimeout:
            time.sleep(random.uniform(2, 4))

    return None


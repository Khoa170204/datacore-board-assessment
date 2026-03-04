# =====================================================
# PROJECT BOOTSTRAP
# Add project root to sys.path so this file can be executed directly.
# Enables absolute imports like `from src.utils import ...`.
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import time
from typing import Any
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
# from src.config import load_config
from src.utils import clean_text, now_vn_iso, setup_logger, load_config

logger = setup_logger("scrape_cafef")

BOARD_TAB_TEXT = "Ban lãnh đạo"
PROFILE_LINK_SELECTOR = "a[href*='/du-lieu/ceo/']"

# =====================================================
# BUILD URL
# Construct company URL from template using exchange and ticker.
# Values are normalized to lowercase before formatting.
def build_url(template: str, exchange: str, ticker: str) -> str:
    return template.format(
        exchange=exchange.lower(),
        ticker=ticker.lower(),
    )

# =====================================================
# PARSE BOARD ROWS
# Extract board member data from raw HTML.
# Steps:
#   - Parse HTML with BeautifulSoup
#   - Locate profile links
#   - Extract person_name and role
#   - Remove duplicate (name, role) pairs
# Returns list of dictionaries.
def parse_board_rows(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for a in soup.select(PROFILE_LINK_SELECTOR):
        name = clean_text(a.get_text(" ", strip=True))
        if not name:
            continue

        card = a.find_parent(
            lambda tag: tag.name == "div"
            and tag.get("onclick")
            and "/du-lieu/ceo/" in tag.get("onclick")
        )

        if card is None:
            card = a.find_parent("div")

        if card is None:
            continue

        role = ""
        for div in card.find_all("div"):
            txt = clean_text(div.get_text(" ", strip=True))
            if not txt:
                continue
            if "tuổi" in txt.lower():
                continue
            if txt == name:
                continue
            role = txt.replace(name, "").strip(" -–—")
            break

        if name and role:
            key = (name, role)
            if key not in seen:
                seen.add(key)
                rows.append({"person_name": name, "role": role})

    return rows

# =====================================================
# SCRAPE SINGLE COMPANY 
# Scrape board data for one company.
# Steps:
#   - Open page with Playwright
#   - Navigate to URL
#   - Click board tab if available
#   - Wait for content to load
#   - Parse board rows
#   - Attach metadata fields
# Returns structured records.
def scrape_one(
    browser,
    template: str,
    exchange: str,
    ticker: str,
) -> list[dict[str, Any]]:

    url = build_url(template, exchange, ticker)
    page = browser.new_page()

    try:
        page.goto(url, wait_until="networkidle", timeout=45000)

        try:
            page.get_by_text(BOARD_TAB_TEXT, exact=False).first.click(timeout=5000)
            page.wait_for_timeout(1200)
        except Exception:
            pass

        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(2000)

        try:
            page.wait_for_selector(PROFILE_LINK_SELECTOR, timeout=5000)
        except:
            pass
        
        html = page.content()
        rows = parse_board_rows(html)

        ts = now_vn_iso()

        return [
            {
                "ticker": ticker.upper(),
                "exchange": exchange.upper(),
                "person_name": r["person_name"],
                "role": r["role"],
                "source": "cafef",
                "scraped_at": ts,
            }
            for r in rows
        ]

    finally:
        page.close()

# =====================================================
# MAIN
# Execute full scraping workflow:
#   - Load configuration
#   - Iterate through tickers
#   - Scrape each company
#   - Apply delay between requests
#   - Aggregate results
#   - Convert timestamps to Vietnam timezone
#   - Save parquet output
def main() -> None:
    cfg = load_config()

    template = cfg["cafef"]["base_url_template"]
    out_path = Path(cfg["output"]["cafef_parquet_path"])
    out_path.parent.mkdir(parents=True, exist_ok=True)

    delay_seconds = float(cfg["request"].get("delay_seconds", 1.2))
    tickers = cfg.get("tickers", [])

    if not tickers:
        raise ValueError("config.yaml: tickers is empty")

    all_records: list[dict[str, Any]] = []
    ok = fail = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        for i, item in enumerate(tickers, start=1):
            exchange = str(item["exchange"])
            ticker = str(item["ticker"])

            logger.info(f"[{i}/{len(tickers)}] {exchange}:{ticker}")

            try:
                recs = scrape_one(
                    browser=browser,
                    template=template,
                    exchange=exchange,
                    ticker=ticker,
                )
                if recs:
                    all_records.extend(recs)
                    ok += 1
                    logger.info(f"  -> rows: {len(recs)}")
                else:
                    logger.warning("  -> rows: 0 (no board found)")
            except Exception as e:
                fail += 1
                logger.error(f"  -> FAILED: {e}")

            time.sleep(delay_seconds)

    df = pd.DataFrame(
        all_records,
        columns=[
            "ticker",
            "exchange",
            "person_name",
            "role",
            "source",
            "scraped_at",
        ],
    )

    df["scraped_at"] = df["scraped_at"]
    
    df.to_parquet(out_path, index=False)

    logger.info(
        f"Done. tickers_ok={ok}, tickers_fail={fail}, total_rows={len(df)}"
    )
    logger.info(f"Wrote: {out_path.resolve()}")


if __name__ == "__main__":
    main()
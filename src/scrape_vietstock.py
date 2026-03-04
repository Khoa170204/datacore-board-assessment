import sys
from pathlib import Path

# =====================================================
# PROJECT BOOTSTRAP
# Add project root to sys.path so this script can be executed directly:
#     python src/scrape_vietstock.py
# Enables absolute imports from src package.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import time
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from src.utils import clean_text, now_vn_iso, setup_logger, load_config, fetch_html

logger = setup_logger("scrape_vietstock")

# =====================================================
# BUILD URL
# Construct Vietstock company URL from template.
# Ticker is normalized to uppercase before formatting.
def build_url(template: str, ticker: str) -> str:
    return template.format(ticker=ticker.upper())

# =====================================================
# PARSE BOARD TABLE
# Extract board member information from HTML.
# Steps:
#   - Locate table containing header "Họ và tên"
#   - Parse tbody rows
#   - Handle optional snapshot time column
#   - Extract person_name and role
#   - Attach metadata fields
# Returns structured records.
def parse_board(html: str, ticker: str, exchange: str):
    soup = BeautifulSoup(html, "html.parser")
    records = []

    board_table = None

    for table in soup.find_all("table"):
        thead = table.find("thead")
        if not thead:
            continue

        headers = [th.get_text(strip=True) for th in thead.find_all("th")]
        if "Họ và tên" in headers:
            board_table = table
            break

    if not board_table:
        return []

    tbody = board_table.find("tbody")
    if not tbody:
        return []

    current_time = None

    for row in tbody.find_all("tr"):
        cols = row.find_all("td")
        if not cols:
            continue

        if len(cols) >= 7:
            current_time = cols[0].get_text(strip=True)
            name = clean_text(cols[1].get_text())
            role = clean_text(cols[2].get_text())
        else:
            name = clean_text(cols[0].get_text())
            role = clean_text(cols[1].get_text())

        if not name:
            continue

        records.append({
            "ticker": ticker.upper(),
            "exchange": exchange.upper(),
            "time_snapshot": current_time,
            "person_name": name,
            "role": role,
            "source": "vietstock",
            "scraped_at": now_vn_iso()
        })

    return records

# =====================================================
# MAIN
# Execute full scraping workflow:
#   - Load configuration from config.yaml
#   - Iterate through tickers
#   - Fetch HTML with retry logic
#   - Parse board table
#   - Apply request delay
#   - Aggregate results
#   - Save output as parquet
def main():
    cfg = load_config()

    template = cfg["vietstock"]["base_url_template"]
    tickers = cfg["tickers"]
    delay = float(cfg["request"].get("delay_seconds", 1.5))
    output_path = Path(cfg["output"]["vietstock_parquet_path"])

    all_records = []
    ok = 0
    fail = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=cfg.get("user_agent")
        )
        page = context.new_page()

        for i, item in enumerate(tickers, start=1):
            ticker = item["ticker"]
            exchange = item["exchange"]

            url = build_url(template, ticker)
            logger.info(f"[{i}/{len(tickers)}] {exchange}:{ticker}")

            try:
                html = fetch_html(page, url)

                if not html:
                    fail += 1
                    logger.warning("  -> FAILED load")
                    continue

                records = parse_board(html, ticker, exchange)

                if records:
                    all_records.extend(records)
                    ok += 1
                    logger.info(f"  -> rows: {len(records)}")
                else:
                    logger.warning("  -> rows: 0")

            except Exception as e:
                fail += 1
                logger.error(f"  -> FAILED: {e}")

            time.sleep(delay)

        browser.close()

    if not all_records:
        logger.error("No data scraped.")
        return

    df = pd.DataFrame(all_records)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, index=False)

    logger.info(
        f"Done. tickers_ok={ok}, tickers_fail={fail}, total_rows={len(df)}"
    )
    logger.info(f"Wrote: {output_path.resolve()}")


if __name__ == "__main__":
    main()
from __future__ import annotations

# --- Bootstrap so this file can be run as:
#     python src/scrape_cafef.py
# even though it imports "src.*"
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from src.config import load_config
from src.utils import clean_text, now_iso, setup_logger

logger = setup_logger("scrape_cafef")

# -----------------------------------------------------------------------------
# Constants: keep all "magic strings/numbers" in one place.
# This makes the scraper easier to tune/debug without hunting through the code.
# -----------------------------------------------------------------------------
VN_TZ = "Asia/Ho_Chi_Minh"

BOARD_TAB_TEXT = "Ban lãnh đạo"
PROFILE_LINK_SELECTOR = "a[href*='/du-lieu/ceo/']"
BOARD_CARD_SELECTOR = (
    "div.directorandonwer_body-directory-person, "
    "div.directorandonwer_body-directory-topperson"
)

# Playwright tuning (milliseconds)
PW_DEFAULT_TIMEOUT_MS = 45_000
PW_CLICK_TIMEOUT_MS = 8_000
PW_AFTER_INTERACTION_WAIT_MS = 1_500
PW_WAIT_CARDS_TIMEOUT_MS = 8_000


def build_url(template: str, exchange: str, ticker: str) -> str:
    """
    Build the target CafeF URL from a template.

    Example template:
      https://cafef.vn/du-lieu/{exchange}/{ticker}-ban-lanh-dao-so-huu.chn

    We lower-case exchange/ticker because CafeF URLs are case-insensitive
    but typically written in lower-case.
    """
    return template.format(exchange=exchange.lower(), ticker=ticker.lower())


def fetch_html(
    session: requests.Session,
    url: str,
    timeout_seconds: int,
    retries: int,
    backoff_seconds: float,
) -> str:
    """
    Fetch raw HTML using Requests, with retry + linear backoff.

    Notes:
    - Some CafeF pages are JS-rendered. Requests may return HTML without the board section.
      That's OK: we will fallback to Playwright later if parsing yields 0 rows.
    - Retries help with transient issues (timeouts, 5xx, throttling).
    """
    last_err: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=timeout_seconds)

            # Fail fast on common cases to keep logs clearer.
            if resp.status_code == 404:
                raise RuntimeError("404 Not Found")
            if resp.status_code >= 500:
                raise RuntimeError(f"Server error {resp.status_code}")

            resp.raise_for_status()
            return resp.text

        except Exception as e:
            last_err = e
            if attempt < retries:
                sleep_s = backoff_seconds * attempt
                logger.info(f"Retry {attempt}/{retries} after error: {e}. Sleep {sleep_s:.1f}s")
                time.sleep(sleep_s)

    raise RuntimeError(f"Failed to fetch: {url}. Last error: {last_err}")


# -----------------------------------------------------------------------------
# Playwright helpers (best-effort)
# We intentionally swallow exceptions here because:
# - CafeF markup can vary per ticker
# - clicking/scrolling is just an attempt to trigger lazy/JS rendering
# - even if it fails, we still want to return page.content() for parsing
# -----------------------------------------------------------------------------
def _best_effort_click_board_tab(page) -> None:
    """Try a few locator strategies to click the 'Ban lãnh đạo' tab (non-fatal)."""
    for locator in [
        page.get_by_text(BOARD_TAB_TEXT, exact=False).first,
        page.locator(f"text={BOARD_TAB_TEXT}").first,
    ]:
        try:
            locator.click(timeout=PW_CLICK_TIMEOUT_MS)
            return
        except Exception:
            pass


def _best_effort_scroll_to_bottom(page) -> None:
    """Scroll to bottom to trigger lazy loading (non-fatal)."""
    try:
        page.evaluate(
            """
            () => {
              const h = Math.max(
                document.body ? document.body.scrollHeight : 0,
                document.documentElement ? document.documentElement.scrollHeight : 0
              );
              window.scrollTo(0, h);
            }
            """
        )
    except Exception:
        pass


def fetch_rendered_html(
    url: str,
    timeout_ms: int = PW_DEFAULT_TIMEOUT_MS,
    user_agent: str = "Mozilla/5.0",
) -> str:
    """
    Fetch fully rendered HTML using Playwright.

    Why this exists:
    - Some CafeF pages render the board/leadership list via JavaScript.
    - Requests can return HTML that *looks valid* but contains none of the board rows.
    """
    with sync_playwright() as p:
        # Headless chromium is enough for most JS-rendered pages.
        browser = p.chromium.launch(headless=True)
        try:
            context = browser.new_context(
                user_agent=user_agent,
                locale="vi-VN",
                viewport={"width": 1366, "height": 768},
            )
            page = context.new_page()

            # 'networkidle' helps ensure most XHR/fetch calls are completed.
            page.goto(url, wait_until="networkidle", timeout=timeout_ms)

            # Ensure DOM exists (avoid "document.body is null" in evaluate()).
            try:
                page.wait_for_selector("body", timeout=timeout_ms)
            except Exception:
                pass

            # Many tickers only populate board cards after switching tab.
            _best_effort_click_board_tab(page)

            # Lazy-load triggers (some pages load more content when scrolled).
            _best_effort_scroll_to_bottom(page)
            page.wait_for_timeout(PW_AFTER_INTERACTION_WAIT_MS)

            # Non-fatal wait for card selectors (improves chance parsing works).
            try:
                page.wait_for_selector(BOARD_CARD_SELECTOR, timeout=PW_WAIT_CARDS_TIMEOUT_MS)
            except Exception:
                pass

            return page.content()
        finally:
            browser.close()


def _extract_role_from_card(card, person_name: str) -> str:
    """
    Extract a role/title from a card DOM element.

    Heuristic:
    - Find the first meaningful <div> text inside the card
    - Skip:
        * empty text
        * the person's name repeated
        * age lines like "50 tuổi"
    """
    for div in card.find_all("div"):
        txt = clean_text(div.get_text(" ", strip=True))
        if not txt:
            continue

        if "tuổi" in txt.lower():
            continue

        if txt == person_name:
            continue

        return txt

    return ""


def _normalize_role(role: str, person_name: str) -> str:
    """
    CafeF sometimes duplicates the name inside the role line, e.g.
      "Bà Mai Kiều Liên Thành viên HĐQT -"
    We remove the name and trim common trailing separators.
    """
    if not role:
        return ""

    role = role.replace(person_name, "").strip()
    role = role.strip(" -–—")
    return role


def _dedup_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Deduplicate exact (person_name, role) pairs while preserving original order."""
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for r in rows:
        key = (r["person_name"], r["role"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def parse_board_rows(html: str) -> list[dict[str, str]]:
    """
    Parse board/leadership data from HTML into:
      [{"person_name": "...", "role": "..."}, ...]

    Important design choice:
    - CafeF layout varies across tickers (CSS classes differ).
    - The most stable signal is profile links containing '/du-lieu/ceo/'.
      So we anchor on that, then walk up to a "card-like" parent to find the role.
    """
    soup = BeautifulSoup(html, "lxml")
    rows: list[dict[str, str]] = []

    for a in soup.select(PROFILE_LINK_SELECTOR):
        name = clean_text(a.get_text(" ", strip=True))
        if not name:
            continue

        # Preferred parent: a clickable div whose onclick contains the profile link.
        # This tends to be the whole "card" container.
        card = a.find_parent(
            lambda tag: tag.name == "div"
            and tag.get("onclick")
            and "/du-lieu/ceo/" in tag.get("onclick")
        )

        # Fallback: some layouts may not have onclick; nearest div is better than nothing.
        if card is None:
            card = a.find_parent("div")

        if card is None:
            continue

        role = _normalize_role(_extract_role_from_card(card, name), name)

        if name and role:
            rows.append({"person_name": name, "role": role})

    return _dedup_rows(rows)


def scrape_one(
    session: requests.Session,
    template: str,
    exchange: str,
    ticker: str,
    timeout_seconds: int,
    retries: int,
    backoff_seconds: float,
) -> list[dict[str, Any]]:
    """
    Scrape one (exchange, ticker) and return records matching the required schema.

    Strategy:
    1) Requests fetch + parse (fast)
    2) If parse yields 0 rows -> Playwright fetch + parse (slower but JS-capable)
    """
    url = build_url(template, exchange, ticker)

    html = fetch_html(session, url, timeout_seconds, retries, backoff_seconds)
    rows = parse_board_rows(html)

    if not rows:
        logger.info("  -> 0 rows from Requests HTML, fallback to Playwright rendering...")
        rendered_html = fetch_rendered_html(
            url,
            timeout_ms=timeout_seconds * 1000,
            user_agent=session.headers.get("User-Agent", "Mozilla/5.0"),
        )
        rows = parse_board_rows(rendered_html)

    # We stamp one timestamp per ticker scrape for consistency.
    ts = now_iso()
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


def main() -> None:
    """
    Entry point:
    - Load config.yaml
    - Scrape all tickers
    - Write Parquet output (with timezone-aware scraped_at in UTC+7)
    """
    cfg = load_config()

    template = cfg["cafef"]["base_url_template"]
    out_path = Path(cfg["output"]["cafef_parquet_path"])
    out_path.parent.mkdir(parents=True, exist_ok=True)

    timeout_seconds = int(cfg["request"].get("timeout_seconds", 20))
    delay_seconds = float(cfg["request"].get("delay_seconds", 1.2))
    retries = int(cfg["request"].get("max_retries", 3))
    backoff_seconds = float(cfg["request"].get("backoff_seconds", 1.0))

    tickers = cfg.get("tickers", [])
    if not tickers:
        raise ValueError("config.yaml: tickers is empty")

    # Reuse a single session for connection pooling + consistent headers.
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": cfg.get("user_agent", "Mozilla/5.0"),
            "Accept-Language": "vi,en;q=0.9",
        }
    )

    all_records: list[dict[str, Any]] = []
    ok = fail = 0

    for i, item in enumerate(tickers, start=1):
        exchange = str(item["exchange"]).lower()
        ticker = str(item["ticker"]).lower()
        logger.info(f"[{i}/{len(tickers)}] {exchange}:{ticker}")

        try:
            recs = scrape_one(
                session=session,
                template=template,
                exchange=exchange,
                ticker=ticker,
                timeout_seconds=timeout_seconds,
                retries=retries,
                backoff_seconds=backoff_seconds,
            )
            all_records.extend(recs)
            ok += 1
            logger.info(f"  -> rows: {len(recs)}")
        except Exception as e:
            fail += 1
            logger.error(f"  -> FAILED: {e}")

        # Be respectful to the site (rate-limiting).
        time.sleep(delay_seconds)

    df = pd.DataFrame(
        all_records,
        columns=["ticker", "exchange", "person_name", "role", "source", "scraped_at"],
    )

    # now_iso() returns an ISO timestamp (typically UTC).
    # Convert to timezone-aware VN time (UTC+7) before saving.
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], utc=True).dt.tz_convert(VN_TZ)

    df.to_parquet(out_path, index=False)

    logger.info(f"Done. tickers_ok={ok}, tickers_fail={fail}, total_rows={len(df)}")
    logger.info(f"Wrote: {out_path.resolve()}")


if __name__ == "__main__":
    main()
# DataCore Technical Assessment

**Vietnamese Board of Directors Dataset**

---

# 1. Overview

In this project, I built a small data pipeline to collect and merge Board of Directors (“Ban lãnh đạo”) data for Vietnamese listed companies.

The goals of this proof-of-concept are:

* Scrape board data from CafeF
* Scrape board data from Vietstock
* Merge and deduplicate both datasets
* Produce a final “golden” dataset with quality indicators

This is a proof-of-concept implementation. The focus is on correctness, structure, reproducibility, and clean engineering practices rather than full coverage of all listed tickers.

---

# 2. Project Structure

```
datacore-board-assessment/

├── README.md
├── requirements.txt
├── config.yaml
│
├── src/
│   ├── scrape_cafef.py
│   ├── scrape_vietstock.py
│   ├── merge.py
│   └── utils.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── final/
│
└── docs/
    ├── data_dictionary.md
    └── data_quality_report.md
```

Scraping logic, merge logic, and utilities are separated to keep the pipeline modular and maintainable.

---

# 3. Setup Instructions

The project was tested on Ubuntu with Python 3.10.

### Step 1 — Clone the repository

```bash
git clone <your-repo-url>
cd datacore-board-assessment
```

### Step 2 — Create virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Step 3 — Install dependencies

```bash
pip install -r requirements.txt
```

### Step 4 — Install Playwright browser

Both CafeF and Vietstock pages are rendered dynamically. Therefore, a headless browser is required:

```bash
playwright install
```

---

# 4. How to Run

These are the exact commands required by the assessment.

### Task 1 — Scrape CafeF

```bash
python src/scrape_cafef.py
```

Output:

```
data/raw/cafef_board.parquet
```

---

### Task 2 — Scrape Vietstock

```bash
python src/scrape_vietstock.py
```

Output:

```
data/raw/vietstock_board.parquet
```

---

### Task 3 — Merge Datasets

```bash
python src/merge.py
```

Output:

```
data/final/board_golden.parquet
```

---

# 5. Technical Approach

## 5.1 CafeF Scraper

Although CafeF board pages look static, parts of the content are rendered dynamically and may change structure over time.

To make the scraper more robust, I used **Playwright (headless Chromium)** to simulate a real browser session.

Process:

* Launch headless browser
* Navigate to the company board page
* Wait for DOM content to load
* Extract rendered HTML
* Parse content using BeautifulSoup
* Apply request delay between tickers (configurable)

Advantages of this approach:

* Handles dynamic content automatically
* Reduces risk of incomplete HTML
* More resilient to minor front-end changes

---

## 5.2 Vietstock Scraper

Vietstock is more complex because:

* It uses CSRF protection
* It requires session cookies
* Some content is rendered via JavaScript

To handle this, I also used **Playwright**.

By using a real browser session:

* Cookies are handled automatically
* CSRF tokens are managed by the browser
* JavaScript-rendered content becomes accessible

After page rendering, I extract the HTML and parse the board table using BeautifulSoup.

---

# 6. Merge & Deduplication Logic

This is the core data engineering component.

## 6.1 Matching Strategy

The datasets are merged using:

```
(ticker, normalized_person_name)
```

Name normalization is necessary because Vietnamese names may vary:

* With or without diacritics
* Extra whitespace
* Honorifics (“Ông”, “Bà”)
* Different casing

The normalization function:

* Removes honorifics
* Removes diacritics
* Converts to lowercase
* Normalizes whitespace

This significantly improves cross-source match rate.

---

## 6.2 Conflict Resolution

When both sources contain the same person:

* If roles differ, the record is marked as `conflict`
* Role from Vietstock is preferred (assumed more structured and updated)
* Agreement metadata is retained

Additional columns:

* `source_agreement`

  * both
  * cafef_only
  * vietstock_only
  * conflict

* `confidence_score`

  * 1.0 → present in both sources with agreement
  * 0.8 → conflict
  * 0.6 → present in only one source

This makes the dataset transparent and auditable.

---

# 7. Data Quality Handling

The pipeline includes:

* Graceful handling of timeouts and missing pages
* Retry logic for dynamic pages
* Configurable delay between requests
* Logging instead of print statements
* Deduplication before merge

If one ticker fails, the pipeline continues processing others.

---

# 8. Configuration

All configurable parameters are stored in `config.yaml`, including:

* Ticker list
* Request delay
* User agent
* Output paths
* Base URL templates

This avoids hardcoding and improves reproducibility.

---

# 9. Known Limitations & Future Improvements

Limitations:

* Matching is rule-based (no fuzzy matching yet)
* Some tickers may fail due to temporary network issues or page structure changes.
* Role comparison is string-based (semantic differences not resolved)
* Snapshot timing differences between sources may reduce match rate

With more time, I would:

* Implement fuzzy name matching (e.g., Levenshtein distance)
* Add automated data quality report generation
* Schedule the pipeline (e.g., daily refresh)
* Store raw HTML snapshots for auditability

---

# 10. Documentation

Additional documentation:

* `docs/data_dictionary.md`
* `docs/data_quality_report.md`

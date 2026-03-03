# DataCore Technical Assessment

## Overview

This project implements Task 1 of the assessment:

Scrape the **Board / Leadership (Ban lãnh đạo)** page from CafeF for Vietnamese listed companies and output the result as a structured Parquet file.

The scraper:

* Uses `requests` as primary fetch method
* Falls back to `Playwright` for JS-rendered pages
* Applies retry + backoff for robustness
* Stores timestamps in **UTC+7 (Asia/Ho_Chi_Minh)**

---

# Environment

Tested on:

* Ubuntu 22.04
* Python 3.10+
* pip

---

# Setup Instructions

## 1️⃣ Clone repository

```bash
git clone <your-repo-url>
cd datacore-board-assessment
```

---

## 2️⃣ Create virtual environment (recommended)

```bash
python3 -m venv .venv
source .venv/bin/activate
```

---

## 3️⃣ Install dependencies

```bash
pip install -r requirements.txt
```

---

## 4️⃣ Install Playwright browser (required)

```bash
python -m playwright install
```

This step is required because some CafeF pages render board data via JavaScript.

---

# Run Task 1 (CafeF Scraper)

Run exactly as required in the evaluation script:

```bash
python src/scrape_cafef.py
```

This command will generate:

```
data/raw/cafef_board.parquet
```

---

# Output Schema

The generated Parquet file contains the following columns:

| Column      | Type     | Description                                  |
| ----------- | -------- | -------------------------------------------- |
| ticker      | str      | Stock ticker symbol                          |
| exchange    | str      | HOSE or HNX                                  |
| person_name | str      | Full name as displayed on CafeF              |
| role        | str      | Original Vietnamese role/title               |
| source      | str      | Always `"cafef"`                             |
| scraped_at  | datetime | ISO 8601 timestamp (UTC+7, Asia/Ho_Chi_Minh) |

---

# Validation (Optional)

You can verify the output with:

```bash
python - <<'PY'
import pandas as pd

df = pd.read_parquet("data/raw/cafef_board.parquet")

print(df.head())
print("rows:", len(df))
print("unique tickers:", df["ticker"].nunique())
print("exchange counts:")
print(df["exchange"].value_counts())
PY
```

---

# Design Decisions

* Use profile links containing `/du-lieu/ceo/` as the primary parsing anchor (more stable than CSS classes).
* Implement retry + linear backoff to handle transient network errors.
* Use a single `requests.Session` for connection pooling.
* Fallback to Playwright when static HTML parsing returns zero rows.
* Convert timestamps to timezone-aware Vietnam time (UTC+7) before saving.

---

# Notes

* Ensure Playwright is installed before running the scraper.
* The scraper includes a configurable delay between requests to respect rate limiting.
* The script is compatible with both:

```bash
python src/scrape_cafef.py
```

and

```bash
python -m src.scrape_cafef
```

---
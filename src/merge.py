import pandas as pd
import re
import unicodedata
from pathlib import Path
from datetime import datetime

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
FINAL_DIR = Path("data/final")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
FINAL_DIR.mkdir(parents=True, exist_ok=True)


# =====================================================
# LOAD RAW DATA

def load_raw():
    """
    Load raw parquet files from both sources.
    """
    cafef = pd.read_parquet(RAW_DIR / "cafef_board.parquet")
    vietstock = pd.read_parquet(RAW_DIR / "vietstock_board.parquet")
    return cafef, vietstock


# =====================================================
# TEXT NORMALIZATION
# Standardize person names to create a matching key across sources.
# The normalization removes honorific prefixes, strips accents,
# converts to lowercase, and removes extra spaces.
# This ensures consistent name matching between datasets.
def normalize_name(name: str) -> str:
    """
    Create normalized name key for matching:
    - Remove Ông/Bà
    - Remove accents
    - Lowercase
    - Remove extra spaces
    """
    if not isinstance(name, str):
        return ""

    name = re.sub(r"^(Ông|Bà)\s+", "", name, flags=re.IGNORECASE)

    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")

    name = re.sub(r"\s+", " ", name.lower())

    return name.strip()


# =====================================================
# CLEAN EACH SOURCE
# Perform an outer join on (ticker, name_key).
# This keeps all records from both sources and allows
# identification of matches and unmatched entries.
def clean_source(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Clean individual source and create matching key.
    """
    df = df.copy()

    df = df[df["person_name"].notna()]
    df["person_name"] = df["person_name"].str.strip()
    df["role"] = df["role"].fillna("").str.strip()

    df["name_key"] = df["person_name"].apply(normalize_name)

    df = df.drop_duplicates(subset=["ticker", "name_key"])

    df.to_parquet(PROCESSED_DIR / f"{source_name}_clean.parquet", index=False)

    return df


# =====================================================
# MERGE
# Perform an outer join on (ticker, name_key).
# This keeps all records from both sources and allows
# identification of matches and unmatched entries.
def merge_sources(cafef: pd.DataFrame,
                  vietstock: pd.DataFrame) -> pd.DataFrame:
    """
    Outer join on (ticker, name_key).
    """
    return cafef.merge(
        vietstock,
        on=["ticker", "name_key"],
        how="outer",
        suffixes=("_cafef", "_vietstock"),
        indicator=True
    )


# =====================================================
# RESOLVE FINAL COLUMNS
# Consolidate duplicated columns from both sources.
# Use non-null values and apply simple priority logic
# (e.g., prefer Vietstock role when available).
def resolve_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Resolve final values for person_name, role, exchange.
    """
    df["person_name"] = df["person_name_cafef"].combine_first(
        df["person_name_vietstock"]
    )

    # Prefer Vietstock role if available
    df["role"] = df["role_vietstock"].combine_first(
        df["role_cafef"]
    )

    df["exchange"] = df["exchange_cafef"].combine_first(
        df["exchange_vietstock"]
    )

    return df


# =====================================================
# ADD AGREEMENT + CONFIDENCE
# Add data quality indicators:
#   - source_agreement: indicates whether a record appears
#     in both sources, only one source, or has conflicts.
#   - confidence_score: numeric confidence based on agreement level.
def add_quality_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add:
    - source_agreement
    - confidence_score
    """

    df["source_agreement"] = "both"

    df.loc[df["_merge"] == "left_only", "source_agreement"] = "cafef_only"
    df.loc[df["_merge"] == "right_only", "source_agreement"] = "vietstock_only"

    conflict_mask = (
        (df["_merge"] == "both") &
        (df["role_cafef"] != df["role_vietstock"])
    )
    df.loc[conflict_mask, "source_agreement"] = "conflict"

    df["confidence_score"] = 1.0
    df.loc[df["source_agreement"] == "conflict", "confidence_score"] = 0.8
    df.loc[df["source_agreement"].isin(
        ["cafef_only", "vietstock_only"]
    ), "confidence_score"] = 0.6

    return df


# =====================================================
# SAVE GOLDEN (FINAL) DATASET

def save_final(df: pd.DataFrame):
    """
    Save final golden dataset.
    """
    final_cols = [
        "ticker",
        "exchange",
        "person_name",
        "role",
        "source_agreement",
        "confidence_score"
    ]

    df_final = df[final_cols].copy()
    df_final["merged_at"] = datetime.now().isoformat()

    df_final.to_parquet(FINAL_DIR / "board_golden.parquet", index=False)


# =====================================================
# MAIN
 
def main():
    print("Loading raw data")
    cafef_raw, vietstock_raw = load_raw()

    print("Cleaning sources")
    cafef = clean_source(cafef_raw, "cafef")
    vietstock = clean_source(vietstock_raw, "vietstock")

    print("Merging")
    merged = merge_sources(cafef, vietstock)

    print("Resolving columns")
    merged = resolve_columns(merged)

    print("Adding quality flags")
    merged = add_quality_flags(merged)

    print("Saving golden (final) dataset")
    save_final(merged)


if __name__ == "__main__":
    main()
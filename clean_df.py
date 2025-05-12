#!/usr/bin/env python3
"""
clean_pipeline.py
=================
Cleans all three datasets required by Sections 4.1 – 4.3:

    • Demographics        →  cleaned_demographics.csv
    • GDP per capita PPP  →  cleaned_gdp.csv         (+ dropped_gdp.csv)
    • Population          →  cleaned_population.csv  (+ dropped_population.csv)

Country-name harmonisation is learned from the demographics step and
re-used for GDP and Population.  All paths are hard-coded near the top.
Run:

    python clean_pipeline.py
"""
from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple
import re
import numpy as np
import pandas as pd

# ────────────────────────────────────────────────────────────────────────────────
# Hard-coded paths  (edit BASE if your folder moves)
# ────────────────────────────────────────────────────────────────────────────────
BASE = Path(r"C:\Josh\Hebrew University\Year2\Intro To Data Science\Exercises\intro_to_ds_ex1")

DEMOG_INPUT = BASE / r"output\demographics_data.csv"
GDP_INPUT = BASE / "gdp_per_capita_2021.csv"
POP_INPUT = BASE / "population_2021.csv"  # ← new

DEMOG_CLEAN = BASE / r"output\cleaned_demographics.csv"
GDP_CLEAN = BASE / r"output\cleaned_gdp.csv"
POP_CLEAN = BASE / r"output\cleaned_population.csv"  # ← new

GDP_DROPPED = BASE / r"output\dropped_gdp.csv"
POP_DROPPED = BASE / r"output\dropped_population.csv"  # ← new
NAME_MISMATCH = BASE / r"output\name_mismatches.csv"

GDP_COL = "GDP_per_capita_PPP"
POP_COL = "Population"  # ← expected column name


# ────────────────────────────────────────────────────────────────────────────────
# Utility helpers
# ────────────────────────────────────────────────────────────────────────────────
def _numericise(df: pd.DataFrame, cols: Tuple[str, ...]) -> None:
    """Convert listed columns to float in-place, coercing errors to NaN."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", ""), errors="coerce")


# ────────────────────────────────────────────────────────────────────────────────
# Country-name normalisation & row-filter helper
# ────────────────────────────────────────────────────────────────────────────────
_SPECIAL_REPLACEMENTS: dict[str, str] = {
    # spelling / synonym fixes so GDP & POP match Demographics
    "Cape Verde": "Cabo Verde",
    "Czechia": "Czech Republic (Czechia)",
    "Swaziland": "Eswatini",
    "East Timor": "Timor-Leste",
    "Saint Vincent And The Grenadines": "St. Vincent & Grenadines",
    "Saint Vincent and The Grenadines": "St. Vincent & Grenadines",
    "United States Virgin Islands": "U.S. Virgin Islands",
    "Sao Tome and Principe": "Sao Tome & Principe",
    "Sao Tome And Principe": "Sao Tome & Principe",
    "Palestine": "State Of Palestine",
    "Cote D'Ivoire": "Côte d'Ivoire",
    "Reunion": "Réunion",
    "Democratic Republic Of Congo": "DR Congo",
    "Curacao": "Curaçao",
}

_CONTINENT_KEYWORDS = (
    "Africa", "America", "Asia", "Europe",
    "Income", "World", "Un)", "(Wb", "\d"
)


def _normalise_country_series(s: pd.Series) -> pd.Series:
    """
    Strip whitespace, drop leading 'the ', Title-Case, then apply the special
    replacements table.  Returns the cleaned Series.
    """
    s = s.astype(str).str.strip()
    s = s.str.replace(r"\s*\(country\)$", "", regex=True, flags=re.I)
    s = s.str.replace(r"^the\s+", "", regex=True, flags=re.I).str.title()
    s = s.replace(_SPECIAL_REPLACEMENTS)
    return s


def _drop_non_country_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes rows whose Country contains continent-level totals, income groups,
    'World', 'UN)', or stray newlines (\n) from the web-crawled sources.
    """
    # Escape each keyword so any special char (like ')') is treated literally:
    escaped = [re.escape(k) for k in _CONTINENT_KEYWORDS]
    # Prepend the newline check, then OR-join them into one pattern:
    pattern = r"\n|" + "|".join(escaped)

    mask_bad = df["Country"].str.contains(
        pattern,
        case=False,
        na=False,
        regex=True
    )
    return df.loc[~mask_bad].copy()


# ────────────────────────────────────────────────────────────────────────────────
# 4 .1  Demographics
# ────────────────────────────────────────────────────────────────────────────────
def clean_demographics() -> Tuple[pd.DataFrame, Dict[str, str]]:
    print("▶ Cleaning demographics …")
    df = pd.read_csv(DEMOG_INPUT)

    # numeric columns
    numeric_cols = tuple(c for c in df.columns if c != "Country")
    _numericise(df, numeric_cols)

    # remove continent / income / World rows
    df = _drop_non_country_rows(df)

    # standardise country names + special spelling fixes
    df["Country_raw"] = df["Country"]
    df["Country"] = _normalise_country_series(df["Country_raw"])

    # drop invalid life-expectancy rows
    le_col = next((c for c in df.columns if "LifeExpectancy" in c and "Both" in c), None)
    if le_col is None:
        raise KeyError("Life-expectancy (Both Sexes) column not found")
    bad = df[le_col].isna() | (df[le_col] < 40) | (df[le_col] > 100) | (df[le_col] < 0)
    if bad.any():
        print(f"   – Dropped {int(bad.sum())} row(s) with invalid life expectancy")
    df = df.loc[~bad].copy()

    mism = df.loc[df["Country_raw"] != df["Country"], ["Country_raw", "Country"]]
    if not mism.empty:
        NAME_MISMATCH.parent.mkdir(parents=True, exist_ok=True)
        mism.to_csv(NAME_MISMATCH, index=False, header=["Original", "Canonical"])
        print(f"   – Logged {len(mism)} name correction(s) → {NAME_MISMATCH.name}")

    name_map = dict(zip(df["Country_raw"], df["Country"], strict=False))

    df = df.drop(columns="Country_raw")

    # Remove collisions like "Micronesia" appearing twice
    df = df.drop_duplicates(subset=["Country"], keep="first")

    # Now set the index
    df = df.set_index("Country").sort_index()
    DEMOG_CLEAN.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(DEMOG_CLEAN)
    print(f"   – Saved cleaned demographics → {DEMOG_CLEAN.name}\n")
    return df, name_map


# ────────────────────────────────────────────────────────────────────────────────
# 4 .2  GDP per capita
# ────────────────────────────────────────────────────────────────────────────────
def clean_gdp(name_map: Dict[str, str]) -> pd.DataFrame:
    print("▶ Cleaning GDP …")
    df = pd.read_csv(GDP_INPUT)

    df = _drop_non_country_rows(df)
    df["Country"] = _normalise_country_series(df["Country"])

    # numeric coercion
    df[GDP_COL] = (
        df[GDP_COL]
        .astype(str)
        .str.replace(r"[^\d.\-]", "", regex=True)
        .replace("", pd.NA)
        .astype(float)
    )

    # b) drop & log missing
    missing = df[GDP_COL].isna()
    if missing.any():
        GDP_DROPPED.parent.mkdir(parents=True, exist_ok=True)
        df.loc[missing].to_csv(GDP_DROPPED, index=False)
        print(f"   – Logged {missing.sum()} row(s) with missing GDP → {GDP_DROPPED.name}")
    df = df.loc[~missing]

    # c) Tukey outliers (raw values)
    q1, q3 = df[GDP_COL].quantile([0.25, 0.75])
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    outliers = ((df[GDP_COL] < lo) | (df[GDP_COL] > hi)).sum()
    print(f"   – Tukey outliers (kept): {int(outliers)}")

    # d) duplicates
    if df.duplicated("Country").any():
        df = df.drop_duplicates("Country", keep="first")
        print("   – Dropped duplicate country rows (kept first)")

    # e) apply mapping
    df["Country"] = df["Country"].replace(name_map)

    df = df.set_index("Country").sort_index()
    GDP_CLEAN.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(GDP_CLEAN)
    print(f"   – Saved cleaned GDP → {GDP_CLEAN.name}\n")
    return df


# ────────────────────────────────────────────────────────────────────────────────
# 4 .3  Population
# ────────────────────────────────────────────────────────────────────────────────
def clean_population(name_map: Dict[str, str]) -> pd.DataFrame:
    print("▶ Cleaning population …")
    df = pd.read_csv(POP_INPUT)

    df = _drop_non_country_rows(df)
    df["Country"] = _normalise_country_series(df["Country"])

    # a) make sure we know which column holds population numbers
    pop_col = "Population"
    if pop_col not in df.columns:
        # fallback – first column that starts with "pop"
        cand = next((c for c in df.columns if c.lower().startswith("pop")), None)
        if cand is None:
            raise KeyError("Population column not found in CSV")
        pop_col = cand

    # numeric coercion (keep digits only, then to float)
    df[pop_col] = (
        df[pop_col]
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)  # strip non-digits
        .replace("", np.nan)  # empty → NaN
        .astype(float)
    )

    # b) drop & log missing
    missing_mask = df[pop_col].isna()
    dropped = int(missing_mask.sum())
    print(f"   – Dropped {dropped} row(s) with missing population data")
    if dropped:
        POP_DROPPED.parent.mkdir(parents=True, exist_ok=True)
        df.loc[missing_mask].to_csv(POP_DROPPED, index=False)
    df = df.loc[~missing_mask].copy()

    # c) log-10 transform → Tukey outlier detection
    log_pop = np.log10(df[pop_col])
    q1, q3 = log_pop.quantile([0.25, 0.75])
    iqr = q3 - q1
    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    outlier_cnt = int(((log_pop < lower) | (log_pop > upper)).sum())
    print(f"   – Tukey outliers after log10 (kept): {outlier_cnt}")

    # d) duplicates + name harmonisation
    if df.duplicated("Country").any():
        df = df.drop_duplicates("Country", keep="first")
        print("   – Dropped duplicate country rows (kept first)")
    df["Country"] = df["Country"].replace(name_map)

    # e) set index & save
    df = df.set_index("Country").sort_index()
    POP_CLEAN.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(POP_CLEAN)
    print(f"   – Saved cleaned population → {POP_CLEAN.name}\n")
    return df


def check_name_matches(
    demo_df: pd.DataFrame,
    gdp_df: pd.DataFrame,
    pop_df: pd.DataFrame
) -> None:
    """
    Prints counts of matching country names between the three cleaned datasets.
    """
    demo_set = set(demo_df.index)
    gdp_set  = set(gdp_df.index)
    pop_set  = set(pop_df.index)

    demo_gdp = demo_set & gdp_set
    demo_pop = demo_set & pop_set
    gdp_pop  = gdp_set & pop_set
    all_three = demo_set & gdp_set & pop_set

    print("🔍 Name‐match summary:")
    print(f" • Demographics ∩ GDP             : {len(demo_gdp)} / {len(demo_set)}")
    print(f" • Demographics ∩ Population      : {len(demo_pop)} / {len(demo_set)}")
    print(f" • GDP ∩ Population               : {len(gdp_pop)} / {len(gdp_set)}")
    print(f" • Intersection of all three      : {len(all_three)}")
    print()
    print("  (Missing in GDP:     ", sorted(demo_set - gdp_set), ")")
    print("  (Missing in Pop:     ", sorted(demo_set - pop_set), ")")
    print("  (In GDP not in demo: ", sorted(gdp_set - demo_set), ")")
    print("  (In Pop not in demo: ", sorted(pop_set - demo_set), ")")


# ────────────────────────────────────────────────────────────────────────────────
# Pipeline
# ────────────────────────────────────────────────────────────────────────────────
def main() -> None:
    demo_df, mapping = clean_demographics()
    gdp_df     = clean_gdp(mapping)
    pop_df     = clean_population(mapping)

    check_name_matches(demo_df, gdp_df, pop_df)
    print("✅ All three datasets cleaned and name‐match checked.")



if __name__ == "__main__":
    main()

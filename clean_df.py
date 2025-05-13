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
from typing import Dict, Tuple, Callable
import re
import numpy as np
import pandas as pd

BASE = Path(r"output")

DEMOG_INPUT = BASE / "demographics_data.csv"
GDP_INPUT = "gdp_per_capita_2021.csv"
POP_INPUT = "population_2021.csv"

DEMOG_CLEAN = BASE / r"cleaned_demographics.csv"
GDP_CLEAN = BASE / r"cleaned_gdp.csv"
POP_CLEAN = BASE / r"cleaned_population.csv"

GDP_DROPPED = BASE / "dropped_gdp.csv"
POP_DROPPED = BASE / "dropped_population.csv"  # ← new
NAME_MISMATCH = BASE / "name_mismatches.csv"

GDP_COL = "GDP_per_capita_PPP"
POP_COL = "Population"  # ← expected column name


def _numericise(df: pd.DataFrame, cols: Tuple[str, ...]) -> None:
    """Convert listed columns to float in-place, coercing errors to NaN."""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", ""), errors="coerce")


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


def coerce_numeric(df: pd.DataFrame, col: str, strip_regex: str) -> pd.DataFrame:
    """Remove unwanted chars via strip_regex, then to float (NaN on errors)."""
    df[col] = (
        df[col].astype(str)
        .str.replace(strip_regex, "", regex=True)
        .replace("", pd.NA)
        .astype(float)
    )
    return df


def drop_and_log_missing(
        df: pd.DataFrame,
        col: str,
        drop_path: Path,
        label: str
) -> pd.DataFrame:
    """Remove rows where df[col] is NaN, write them to drop_path (even if empty),
    and print count."""
    mask = df[col].isna()
    # ensure the output folder exists
    drop_path.parent.mkdir(parents=True, exist_ok=True)

    # write out whichever rows are “missing” (zero-row DataFrame still writes headers)
    df.loc[mask].to_csv(drop_path, index=False)

    count = int(mask.sum())
    if count:
        print(f"   – Dropped {count} missing-{label} rows → {drop_path.name}")
    else:
        print(f"   – No missing-{label} rows; created empty → {drop_path.name}")

    # return the rows that were not NaN
    return df.loc[~mask].copy()


def report_outliers(
        df: pd.DataFrame,
        col: str,
        transform: Callable[[pd.Series], pd.Series],
        label: str
) -> None:
    """Compute Tukey fences on transform(df[col]) and print how many fall outside."""
    data = transform(df[col])
    q1, q3 = data.quantile([0.25, 0.75])
    iqr = q3 - q1
    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    outlier_count = int(((data < lower) | (data > upper)).sum())
    print(f"   – {label} outliers (kept): {outlier_count}")


def dedupe_countries(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only the first row for each country."""
    dupes = df.duplicated("Country")
    if dupes.any():
        print(f"   – Removing {int(dupes.sum())} duplicate country rows")
        df = df.drop_duplicates("Country", keep="first")
    return df


def apply_name_map(df: pd.DataFrame, name_map: Dict[str, str]) -> pd.DataFrame:
    """Overwrite df['Country'] where name_map has a key."""
    df["Country"] = df["Country"].replace(name_map)
    return df


def save_df(df: pd.DataFrame, path: Path, index: bool = True) -> None:
    """Ensure parent exists, write to CSV."""
    path.parent.mkdir(exist_ok=True, parents=True)
    df.to_csv(path, index=index)
    print(f"   – Saved → {path.name}")


def clean_demographics() -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Cleans the raw demographics data:
      1. Reads the input CSV.
      2. Converts all non-Country columns to numeric.
      3. Drops continent/income/‘World’ rows.
      4. Normalises country names and logs any mismatches.
      5. Removes rows with invalid life-expectancy values.
      6. Deduplicates, sets Country as index, saves to cleaned_demographics.csv.
    Returns:
      - cleaned DataFrame (indexed by Country)
      - name_map dict from original → canonical country name
    """
    print("Cleaning demographics …")
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


def clean_gdp(name_map: Dict[str, str]) -> pd.DataFrame:
    """
    Cleans the GDP per-capita dataset:
      1. Reads the raw CSV.
      2. Drops non-country rows and normalises country names.
      3. Converts the GDP column to float.
      4. Logs & removes any missing-GDP rows (always writes dropped_gdp.csv).
      5. Reports Tukey outliers, deduplicates, applies the demographics name_map.
      6. Sets Country as index, saves to cleaned_gdp.csv, and returns the DataFrame.
    """
    df = pd.read_csv(GDP_INPUT)
    print("GDP:")
    df = _drop_non_country_rows(df)
    df["Country"] = _normalise_country_series(df["Country"])
    df = coerce_numeric(df, GDP_COL, r"[^\d\.\-]")
    df = drop_and_log_missing(df, GDP_COL, GDP_DROPPED, "GDP")
    report_outliers(df, GDP_COL, lambda s: s, "GDP")
    df = dedupe_countries(df)
    df = apply_name_map(df, name_map)
    df = df.set_index("Country").sort_index()
    save_df(df, GDP_CLEAN)
    return df


def clean_population(name_map: Dict[str, str]) -> pd.DataFrame:
    """
    Cleans the Population dataset:
      1. Reads the raw CSV.
      2. Drops non-country rows and normalises country names.
      3. Converts the Population column to numeric.
      4. Logs & removes any missing-Population rows (always writes dropped_population.csv).
      5. Reports Tukey outliers on log10 scale, deduplicates, applies the demographics name_map.
      6. Sets Country as index, saves to cleaned_population.csv, and returns the DataFrame.
    """
    df = pd.read_csv(POP_INPUT)
    print("Population:")
    df = _drop_non_country_rows(df)
    df["Country"] = _normalise_country_series(df["Country"])
    df = coerce_numeric(df, POP_COL, r"[^\d]")
    df = drop_and_log_missing(df, POP_COL, POP_DROPPED, "Population")
    report_outliers(df, POP_COL, np.log10, "Population")
    df = dedupe_countries(df)
    df = apply_name_map(df, name_map)
    df = df.set_index("Country").sort_index()
    save_df(df, POP_CLEAN)
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
    gdp_set = set(gdp_df.index)
    pop_set = set(pop_df.index)

    demo_gdp = demo_set & gdp_set
    demo_pop = demo_set & pop_set
    gdp_pop = gdp_set & pop_set
    all_three = demo_set & gdp_set & pop_set

    print("Name‐match summary:")
    print(f" • Demographics ∩ GDP             : {len(demo_gdp)} / {len(demo_set)}")
    print(f" • Demographics ∩ Population      : {len(demo_pop)} / {len(demo_set)}")
    print(f" • GDP ∩ Population               : {len(gdp_pop)} / {len(gdp_set)}")
    print(f" • Intersection of all three      : {len(all_three)}")
    print()
    print("  (Missing in GDP:     ", sorted(demo_set - gdp_set), ")")
    print("  (Missing in Pop:     ", sorted(demo_set - pop_set), ")")
    print("  (In GDP not in demo: ", sorted(gdp_set - demo_set), ")")
    print("  (In Pop not in demo: ", sorted(pop_set - demo_set), ")")


def main() -> None:
    demo_df, mapping = clean_demographics()
    gdp_df = clean_gdp(mapping)
    pop_df = clean_population(mapping)

    check_name_matches(demo_df, gdp_df, pop_df)
    print("All three datasets cleaned and name‐match checked.")


if __name__ == "__main__":
    main()

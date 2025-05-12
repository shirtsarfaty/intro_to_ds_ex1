
#!/usr/bin/env python3
"""
clean_df.py
===============

Utility script that cleans a *GDP‑per‑capita (PPP)* dataset according to the
specification in Section4.2 of the assignment:

    a. Ensure the GDP column is numeric (remove commas, symbols, etc.).
    b. Drop rows whose GDP is missing (None/NaN) and log them.
    c. Identify Tukey outliers (values < Q1–1.5·IQR or >Q3+1.5·IQR).
       • Only *report* their count – do **not** remove them.
    d. Handle duplicate country names (policy: keep first|last|mean).
    e. Apply the same country‑name mapping used for the demographics dataset.
    f. Set Country as the DataFrame index and write the cleaned CSV.

Run:

    python clean_df.py --input gdp_per_capita_2021.csv

Optional flags:

    --output-dir           Where to place cleaned & dropped CSVs (default: output)
    --name-map             CSV with 2 columns (raw,canonical) for harmonising names
    --duplicate-policy     first | last | mean  (default: first)

The cleaned file is saved as  <output‑dir>/cleaned_gdp.csv
Rows dropped due to missing GDP go to <output‑dir>/dropped_gdp.csv
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd

GDP_COL = "GDP_per_capita_PPP"


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------
def clean_gdp(
    input_path: Path,
    output_clean_path: Path,
    output_dropped_path: Path,
    country_name_mapping: dict[str, str] | None = None,
    duplicate_policy: str = "first",
) -> pd.DataFrame:
    """Clean a GDP‑per‑capita CSV and return the resulting DataFrame.

    Parameters
    ----------
    input_path
        Raw CSV containing at least 'Country' and GDP column.
    output_clean_path
        Destination CSV for the cleaned DataFrame.
    output_dropped_path
        Destination CSV for rows removed because GDP was missing.
    country_name_mapping
        Mapping from raw → canonical country names to harmonise datasets.
    duplicate_policy
        How to handle duplicate country entries:
        • 'first'  – keep the first row
        • 'last'   – keep the last row
        • 'mean'   – keep the mean GDP value of duplicates
    """
    # 1. Read
    df = pd.read_csv(input_path)

    # 2. GDP to numeric (strip commas, currency symbols, spaces, etc.)
    df[GDP_COL] = (
        df[GDP_COL]
        .astype(str)
        .str.replace(r"[^0-9.\-]", "", regex=True)  # keep digits/dot/minus
        .replace("", pd.NA)  # empty strings → NA
        .astype(float)
    )

    # 3. Drop & log missing GDP rows
    missing_mask = df[GDP_COL].isna()
    if missing_mask.any():
        output_dropped_path.parent.mkdir(parents=True, exist_ok=True)
        df.loc[missing_mask].to_csv(output_dropped_path, index=False)
    df = df.loc[~missing_mask]

    # 4. Harmonise country names
    if country_name_mapping:
        df["Country"] = df["Country"].replace(country_name_mapping)

    # 5. Detect Tukey outliers (NOT dropped)
    q1, q3 = df[GDP_COL].quantile([0.25, 0.75])
    iqr = q3 - q1
    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    outlier_count = int(((df[GDP_COL] < lower) | (df[GDP_COL] > upper)).sum())
    print(f"Tukey outlier count (not removed): {outlier_count}")

    # 6. Deduplicate
    if df.duplicated("Country").any():
        if duplicate_policy == "first":
            df = df.drop_duplicates("Country", keep="first")
        elif duplicate_policy == "last":
            df = df.drop_duplicates("Country", keep="last")
        elif duplicate_policy == "mean":
            df = (
                df.groupby("Country", as_index=False)[GDP_COL]
                .mean(numeric_only=True)
                .sort_values("Country")
            )
        else:
            raise ValueError(f"Unknown duplicate_policy '{duplicate_policy}'")  # noqa: TRY003

    # 7. Set index and write out
    df = df.set_index("Country").sort_index()

    output_clean_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_clean_path)

    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser("""Clean GDP‑per‑capita dataset""")
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help="Raw GDP per capita CSV (must contain 'Country' & GDP column)"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory for cleaned & dropped CSVs (default: ./output)"
    )
    parser.add_argument(
        "--name-map",
        type=Path,
        help="Optional CSV with two columns: raw,canonical – for country name mapping"
    )
    parser.add_argument(
        "--duplicate-policy",
        choices=["first", "last", "mean"],
        default="first",
        help="How to resolve duplicate country entries (default: first)"
    )
    return parser.parse_args()


def load_name_mapping(map_path: Path | None) -> dict[str, str] | None:
    if map_path is None:
        return None
    df_map = pd.read_csv(map_path, header=None, names=["raw", "canonical"])
    return dict(zip(df_map["raw"], df_map["canonical"], strict=False))


def main() -> None:
    args = parse_args()

    mapping = load_name_mapping(args.name_map)

    clean_gdp(
        input_path=args.input,
        output_clean_path=args.output_dir / "cleaned_gdp.csv",
        output_dropped_path=args.output_dir / "dropped_gdp.csv",
        country_name_mapping=mapping,
        duplicate_policy=args.duplicate_policy,
    )


if __name__ == "__main__":
    main()

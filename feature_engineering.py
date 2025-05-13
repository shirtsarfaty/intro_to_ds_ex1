"""
feature_engineering.py
Part 3 of the project.

Reads:
    output/cleaned_demographics.csv
    output/cleaned_gdp.csv
    output/cleaned_population.csv

Produces:
    output/missing_values.csv      (rows with zero in any numeric column)
    output/merged_dataset.csv      (inner-joined master table)
    output/lost_countries.csv      (countries dropped by the join)
    output/X.npy                   (final NumPy feature matrix)
"""

from pathlib import Path
import numpy as np
import pandas as pd

BASE = Path("output")
DEMOG_CLEAN = BASE / "cleaned_demographics.csv"
GDP_CLEAN = BASE / "cleaned_gdp.csv"
POP_CLEAN = BASE / "cleaned_population.csv"

MISSING_VALUES = BASE / "missing_values.csv"
MERGED_CSV = BASE / "merged_dataset.csv"
LOST_COUNTRIES = BASE / "lost_countries.csv"
FEATURE_NPY = BASE / "X.npy"

# Column labels used later
GDP_PC_COL = "GDP_per_capita_PPP"
POP_COL = "Population"
LE_BOTH_COL = "LifeExpectancy_Both"


def zscore(col: pd.Series) -> pd.Series:
    """Return (x - μ) / σ using population σ (ddof=0)."""
    mu = col.mean()
    std = col.std(ddof=0)
    return (col - mu) / std


def add_total_gdp(df: pd.DataFrame) -> pd.DataFrame:
    """Create TotalGDP = GDP_per_capita_PPP × Population."""
    df["TotalGDP"] = df[GDP_PC_COL] * df[POP_COL]
    return df


def add_log_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add LogGDPperCapita and LogPopulation (base-10)."""
    df["LogGDPperCapita"] = np.log10(df[GDP_PC_COL])
    df["LogPopulation"] = np.log10(df[POP_COL])
    return df


def zscore_selected(df: pd.DataFrame) -> pd.DataFrame:
    """Z-score LifeExpectancy_Both, LogGDPperCapita, LogPopulation."""
    cols = [LE_BOTH_COL, "LogGDPperCapita", "LogPopulation"]
    df[cols] = df[cols].apply(zscore)
    return df


def integrate_and_save(df_demo, df_gdp, df_pop) -> pd.DataFrame:
    """
    1) Inner-join
    2) Report lost countries
    3) Treat any zero in any numeric column as missing:
       • save those rows → missing_values.csv
       • replace zeros with the column’s non-zero mean
    4) Fill any residual NaNs with column means
    5) Save merged_dataset.csv
    """
    # join
    df_final = df_demo.join(df_gdp, how="inner").join(df_pop, how="inner")

    # lost countries report
    joined = set(df_final.index)
    universe = set(df_demo.index) | set(df_gdp.index) | set(df_pop.index)
    lost = sorted(universe - joined)
    print(f"Countries after inner join: {len(joined)} (lost {len(lost)})")
    if lost:
        pd.Series(lost, name="LostCountry") \
            .to_csv(LOST_COUNTRIES, index=False)
        print(f"  – Lost list → {LOST_COUNTRIES.name}")

    # Find numeric columns and cast them to float
    num_cols = df_final.select_dtypes(include="number").columns
    df_final[num_cols] = df_final[num_cols].astype("float64")

    # detect zeros in any numeric column
    num_cols = df_final.select_dtypes(include="number").columns
    zero_mask = (df_final[num_cols] == 0).any(axis=1)
    if zero_mask.any():
        # save all rows that had at least one 0
        df_final.loc[zero_mask].to_csv(MISSING_VALUES)
        print(f"  – Rows with zeros → {MISSING_VALUES.name}")

        # replace zeros with mean over non-zeros
        for col in num_cols:
            nonzeros = df_final.loc[df_final[col] != 0, col]
            if not nonzeros.empty:
                mean_val = nonzeros.mean()
                df_final.loc[df_final[col] == 0, col] = mean_val

    # fill any remaining NaNs
    for col in num_cols:
        if df_final[col].isna().any():
            df_final[col].fillna(df_final[col].mean(), inplace=True)

    # save merged
    df_final.to_csv(MERGED_CSV)
    print(f"Full merged dataset → {MERGED_CSV.name}")
    return df_final


def main() -> None:
    # load cleaned data
    demo = pd.read_csv(DEMOG_CLEAN, index_col="Country")
    gdp = pd.read_csv(GDP_CLEAN, index_col="Country")
    pop = pd.read_csv(POP_CLEAN, index_col="Country")

    # build TotalGDP in GDP table
    gdp = gdp.join(pop[[POP_COL]], how="left")
    gdp = add_total_gdp(gdp).drop(columns=[POP_COL])

    # integrate, zero-logic, and save
    merged = integrate_and_save(demo, gdp, pop)

    # add log & z-score features
    merged = add_log_features(merged)
    merged = zscore_selected(merged)

    # build feature matrix and save
    features = [LE_BOTH_COL, "LogGDPperCapita", "LogPopulation"]
    X = merged.loc[:, features].sort_index()
    np.save(FEATURE_NPY, X.to_numpy().astype(np.float32))
    print(
        f"Feature matrix → {FEATURE_NPY.name}; "
        f"shape={X.shape}; cols={list(X.columns)}"
    )

    merged.describe().to_csv(
        BASE / "demo_analysis_after_engineering.csv",
        encoding="utf-8-sig"
    )


if __name__ == "__main__":
    main()

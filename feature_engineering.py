"""
feature_engineering.py
======================
Part 3 of the project.

Reads:
    output/cleaned_demographics.csv
    output/cleaned_gdp.csv
    output/cleaned_population.csv

Produces:
    output/merged_dataset.csv        (inner-joined master table)
    output/lost_countries.csv        (countries dropped by the join)
    output/X.npy                     (final NumPy feature matrix)
"""

from pathlib import Path
import numpy as np
import pandas as pd


BASE          = Path(r"C:\Josh\Hebrew University\Year2\Intro To Data Science\Exercises\intro_to_ds_ex1\output")
DEMOG_CLEAN   = BASE / "cleaned_demographics.csv"
GDP_CLEAN     = BASE / "cleaned_gdp.csv"
POP_CLEAN     = BASE / "cleaned_population.csv"

MERGED_CSV    = BASE / "merged_dataset.csv"
LOST_COUNTRIES= BASE / "lost_countries.csv"
FEATURE_NPY   = BASE / "X.npy"

# Column labels used later
GDP_PC_COL    = "GDP_per_capita_PPP"
POP_COL       = "Population"
LE_BOTH_COL   = "LifeExpectancy_Both"


def zscore(col: pd.Series) -> pd.Series:
    """Return (x - μ) / σ   using population σ (ddof=0)."""
    mu  = col.mean()
    std = col.std(ddof=0)
    return (col - mu) / std


def add_total_gdp(df: pd.DataFrame) -> pd.DataFrame:
    """Create TotalGDP = GDP_per_capita_PPP × Population."""
    df["TotalGDP"] = df[GDP_PC_COL] * df[POP_COL]
    return df


def add_log_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add LogGDPperCapita and LogPopulation (base-10)."""
    df["LogGDPperCapita"] = np.log10(df[GDP_PC_COL])
    df["LogPopulation"]   = np.log10(df[POP_COL])
    return df


def zscore_selected(df: pd.DataFrame) -> pd.DataFrame:
    """Z-score LifeExpectancy_Both, LogGDPperCapita, LogPopulation."""
    cols_to_norm = [LE_BOTH_COL, "LogGDPperCapita", "LogPopulation"]
    df[cols_to_norm] = df[cols_to_norm].apply(zscore)
    return df


def integrate_and_save(df_demo, df_gdp, df_pop) -> pd.DataFrame:
    """Inner-join, report lost countries, fill residual NaNs with means."""
    df_final = df_demo.join(df_gdp, how="inner").join(df_pop, how="inner")

    joined   = set(df_final.index)
    universe = set(df_demo.index) | set(df_gdp.index) | set(df_pop.index)
    lost     = sorted(universe - joined)

    print(f"Countries after inner join: {len(joined)}  (lost {len(lost)})")
    if lost:
        pd.Series(lost, name="LostCountry").to_csv(LOST_COUNTRIES, index=False)
        print(f"  – List saved to {LOST_COUNTRIES.name}")

    # fill any remaining numeric NaNs with column means
    for col in df_final.select_dtypes(include=["number"]):
        if df_final[col].isna().any():
            df_final[col].fillna(df_final[col].mean(), inplace=True)

    df_final.to_csv(MERGED_CSV)
    print(f"Full merged dataset → {MERGED_CSV.name}")
    return df_final


def main() -> None:
    # 1) load cleaned files (all have Country as index already)
    demo = pd.read_csv(DEMOG_CLEAN, index_col="Country")
    gdp  = pd.read_csv(GDP_CLEAN,   index_col="Country")
    pop  = pd.read_csv(POP_CLEAN,   index_col="Country")

    # 2) add TotalGDP to GDP × Population combo (require population absolute)
    gdp  = gdp.join(pop[[POP_COL]], how="left")
    gdp  = add_total_gdp(gdp).drop(columns=[POP_COL])   # only keep TotalGDP new col

    # 3) log10 transforms & z-scaling happen after merging so means/σ align
    merged = integrate_and_save(demo, gdp, pop)
    merged = add_log_features(merged)
    merged = zscore_selected(merged)

    # 4) Build NumPy feature matrix (rows sorted alphabetically by Country)
    feature_cols = [LE_BOTH_COL, "LogGDPperCapita", "LogPopulation"]
    X = merged.loc[:, feature_cols].values.astype(np.float32)
    np.save(FEATURE_NPY, X)
    print(f"Feature matrix saved → {FEATURE_NPY.name}  shape={X.shape}")


if __name__ == "__main__":
    main()

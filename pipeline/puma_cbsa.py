"""Build a PUMA → CBSA (metro/micro) crosswalk for the 2024 PUMS.

Joins two Census reference files:

  1. 2020 Tract → 2020 PUMA — gives each tract's PUMA. The county is
     derivable from STATEFP+COUNTYFP at the front of the tract id.
     Source: 2020_Census_Tract_to_2020_PUMA.txt (CSV, ~1.7 MB)

  2. OMB / Census 2023 CBSA delineation — gives each county its CBSA
     (Metropolitan or Micropolitan statistical area) if any.
     Source: list1_2023.xls (~1 MB; needs xlrd in the venv)

We approximate (state, PUMA) → CBSA by counting tracts per
(state, PUMA, county), joining counties to CBSAs, and picking the CBSA
with the most tracts in each PUMA. Tracts are designed to be roughly
equal in population so this is a reasonable proxy when actual
population weights aren't easily available. PUMAs whose dominant county
is non-CBSA fall back to "Non-metro / rural <state>".

Output: data/parquet/puma_cbsa_crosswalk.parquet
Columns: state_fips, puma, cbsa_code, cbsa_name, share
"""
from __future__ import annotations

import io
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet"

TRACT_PUMA_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
    "2020_Census_Tract_to_2020_PUMA.txt"
)
CBSA_DELINEATION_URL = (
    "https://www2.census.gov/programs-surveys/metro-micro/geographies/"
    "reference-files/2023/delineation-files/list1_2023.xlsx"
)

TRACT_PUMA_RAW = RAW_DIR / "2020_Census_Tract_to_2020_PUMA.txt"
CBSA_DELINEATION_RAW = RAW_DIR / "list1_2023.xlsx"
OUT_PATH = PARQUET_DIR / "puma_cbsa_crosswalk.parquet"

STATE_FIPS_TO_NAME = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut", "10": "Delaware",
    "11": "District of Columbia", "12": "Florida", "13": "Georgia",
    "15": "Hawaii", "16": "Idaho", "17": "Illinois", "18": "Indiana",
    "19": "Iowa", "20": "Kansas", "21": "Kentucky", "22": "Louisiana",
    "23": "Maine", "24": "Maryland", "25": "Massachusetts", "26": "Michigan",
    "27": "Minnesota", "28": "Mississippi", "29": "Missouri", "30": "Montana",
    "31": "Nebraska", "32": "Nevada", "33": "New Hampshire", "34": "New Jersey",
    "35": "New Mexico", "36": "New York", "37": "North Carolina",
    "38": "North Dakota", "39": "Ohio", "40": "Oklahoma", "41": "Oregon",
    "42": "Pennsylvania", "44": "Rhode Island", "45": "South Carolina",
    "46": "South Dakota", "47": "Tennessee", "48": "Texas", "49": "Utah",
    "50": "Vermont", "51": "Virginia", "53": "Washington", "54": "West Virginia",
    "55": "Wisconsin", "56": "Wyoming",
}


def download(url: str, dest: Path) -> Path:
    if dest.exists():
        print(f"  cached: {dest.name} ({dest.stat().st_size:,} bytes)")
        return dest
    print(f"  GET {url}")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(resp.content)
    print(f"  saved: {dest.name} ({dest.stat().st_size:,} bytes)")
    return dest


def load_tract_puma() -> pd.DataFrame:
    """Returns df with columns: state_fips, puma, county_fips (one row per tract)."""
    download(TRACT_PUMA_URL, TRACT_PUMA_RAW)
    df = pd.read_csv(TRACT_PUMA_RAW, dtype=str, encoding="utf-8-sig")
    # Cols: STATEFP, COUNTYFP, TRACTCE, PUMA5CE
    df = df.rename(columns={"STATEFP": "state_fips", "COUNTYFP": "county_fips_3", "PUMA5CE": "puma"})
    df["state_fips"] = df["state_fips"].str.zfill(2)
    df["puma"] = df["puma"].str.zfill(5)
    df["county_fips_3"] = df["county_fips_3"].str.zfill(3)
    df["county_fips"] = df["state_fips"] + df["county_fips_3"]
    return df[["state_fips", "puma", "county_fips"]]


def load_county_cbsa() -> pd.DataFrame:
    """Returns df with columns: county_fips, cbsa_code, cbsa_name."""
    download(CBSA_DELINEATION_URL, CBSA_DELINEATION_RAW)
    # The OMB delineation xls has 2 title rows then a header row; counties are
    # listed by state. xlrd reads .xls.
    try:
        df = pd.read_excel(CBSA_DELINEATION_RAW, header=2, dtype=str, engine="openpyxl")
    except ImportError as e:
        sys.exit(f"missing openpyxl in venv: {e}\n"
                 f"Install with: .venv/bin/pip install openpyxl")

    # Column names vary across years — find them dynamically.
    cols = {c.lower().strip(): c for c in df.columns}
    cbsa_code_col = cols.get("cbsa code")
    cbsa_name_col = cols.get("cbsa title")
    state_fips_col = cols.get("fips state code")
    county_fips_col = cols.get("fips county code")
    if not all([cbsa_code_col, cbsa_name_col, state_fips_col, county_fips_col]):
        sys.exit(f"unexpected CBSA file columns: {list(df.columns)}")

    df = df[[cbsa_code_col, cbsa_name_col, state_fips_col, county_fips_col]].rename(
        columns={
            cbsa_code_col: "cbsa_code",
            cbsa_name_col: "cbsa_name",
            state_fips_col: "_st",
            county_fips_col: "_co",
        }
    )
    # Drop trailing footnote rows that have no FIPS values.
    df = df.dropna(subset=["_st", "_co", "cbsa_code"])
    df["county_fips"] = df["_st"].str.zfill(2) + df["_co"].str.zfill(3)
    return df[["county_fips", "cbsa_code", "cbsa_name"]]


def build_crosswalk() -> pd.DataFrame:
    tract_puma = load_tract_puma()
    county_cbsa = load_county_cbsa()

    print(f"  tract→(puma, county) rows: {len(tract_puma):,}")
    print(f"  county→cbsa rows: {len(county_cbsa):,}")

    merged = tract_puma.merge(county_cbsa, on="county_fips", how="left")
    nonmetro_mask = merged["cbsa_code"].isna()
    merged.loc[nonmetro_mask, "cbsa_code"] = (
        "NM_" + merged.loc[nonmetro_mask, "state_fips"]
    )
    merged.loc[nonmetro_mask, "cbsa_name"] = (
        "Non-metro / rural " + merged.loc[nonmetro_mask, "state_fips"].map(STATE_FIPS_TO_NAME)
    )

    # Tract counts per (state, puma, cbsa) — proxy for population since
    # tracts are designed to be roughly equal-population.
    grouped = (
        merged.groupby(["state_fips", "puma", "cbsa_code", "cbsa_name"], as_index=False)
        .size()
        .rename(columns={"size": "tract_count"})
    )
    grouped["puma_total"] = grouped.groupby(["state_fips", "puma"])["tract_count"].transform("sum")
    grouped["share"] = (grouped["tract_count"] / grouped["puma_total"]).round(4)

    grouped = grouped.sort_values(["state_fips", "puma", "share"], ascending=[True, True, False])
    primary = grouped.drop_duplicates(subset=["state_fips", "puma"], keep="first")
    out = primary[["state_fips", "puma", "cbsa_code", "cbsa_name", "share"]].reset_index(drop=True)
    return out


def main() -> None:
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    df = build_crosswalk()

    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        OUT_PATH,
        compression="zstd",
    )
    rel = OUT_PATH.relative_to(PROJECT_ROOT)
    print(f"  rows: {len(df):,} (PUMAs)")
    print(f"  parquet: {rel} ({OUT_PATH.stat().st_size:,} bytes)")
    print()
    print("  CBSA coverage by share:")
    print((df["share"] >= 0.95).sum(), "PUMAs with single dominant CBSA (≥95%)")
    print((df["share"] >= 0.5).sum(), "PUMAs with majority CBSA (≥50%)")
    print(df["cbsa_code"].str.startswith("NM_").sum(), "PUMAs assigned non-metro fallback")


if __name__ == "__main__":
    main()

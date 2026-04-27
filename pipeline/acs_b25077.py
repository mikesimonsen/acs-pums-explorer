"""Fetch ACS B25077 (median home value) for all US counties from the
2020-2024 5-year ACS and write to Parquet.

Output: data/parquet/acs_b25077_county_2020_2024.parquet
Columns: state_fips, county_fips, geo_id, state_name, county_name,
         median_home_value, median_home_value_moe
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

ACS_END_YEAR = 2024  # 5-year ACS endpoint year = end of 5-year window
ACS_DATASET = "acs/acs5"
TABLE = "B25077"
VAR_ESTIMATE = f"{TABLE}_001E"
VAR_MOE = f"{TABLE}_001M"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet"
RAW_PATH = RAW_DIR / f"acs_b25077_county_{ACS_END_YEAR - 4}_{ACS_END_YEAR}.json"
OUTPUT = PARQUET_DIR / f"acs_b25077_county_{ACS_END_YEAR - 4}_{ACS_END_YEAR}.parquet"


def fetch(api_key: str) -> list:
    url = f"https://api.census.gov/data/{ACS_END_YEAR}/{ACS_DATASET}"
    params = {
        "get": f"NAME,{VAR_ESTIMATE},{VAR_MOE}",
        "for": "county:*",
        "key": api_key,
    }
    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()


def to_dataframe(rows: list) -> pd.DataFrame:
    header, *data = rows
    df = pd.DataFrame(data, columns=header)

    # NAME comes back as "County Name, State Name"
    name_parts = df["NAME"].str.split(", ", n=1, expand=True)
    df["county_name"] = name_parts[0]
    df["state_name"] = name_parts[1]

    df["state_fips"] = df["state"]
    df["county_fips"] = df["county"]
    df["geo_id"] = df["state"] + df["county"]

    # ACS uses negative sentinels (-666666666, -222222222, -888888888) for
    # not-available / not-computable / not-applicable. Coerce to null.
    for src, dst in [(VAR_ESTIMATE, "median_home_value"), (VAR_MOE, "median_home_value_moe")]:
        col = pd.to_numeric(df[src], errors="coerce")
        col = col.where(col >= 0)
        df[dst] = col.astype("Int64")

    cols = [
        "state_fips",
        "county_fips",
        "geo_id",
        "state_name",
        "county_name",
        "median_home_value",
        "median_home_value_moe",
    ]
    return df[cols].sort_values("geo_id").reset_index(drop=True)


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    api_key = os.environ.get("CENSUS_API_KEY")
    if not api_key:
        sys.exit("CENSUS_API_KEY missing — set it in .env")

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {TABLE} from {ACS_END_YEAR - 4}-{ACS_END_YEAR} 5-year ACS...")
    rows = fetch(api_key)

    RAW_PATH.write_text(json.dumps(rows))
    print(f"  raw: {RAW_PATH.relative_to(PROJECT_ROOT)} ({RAW_PATH.stat().st_size:,} bytes)")

    df = to_dataframe(rows)
    print(f"  counties: {len(df)}")
    print(f"  null estimates: {df['median_home_value'].isna().sum()}")

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, OUTPUT, compression="zstd")
    print(f"  parquet: {OUTPUT.relative_to(PROJECT_ROOT)} ({OUTPUT.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()

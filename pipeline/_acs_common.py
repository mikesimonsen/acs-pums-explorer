"""Shared helpers for ACS ingestion scripts.

One Parquet per table, county-level, 2020-2024 5-year ACS. Each table
script in this package supplies:
  - the table id
  - the variables to fetch (estimate + MOE columns)
  - a rename map from ACS variable id to friendly column name
  - optional derived columns
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Dict, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv

ACS_END_YEAR = 2024
ACS_DATASET = "acs/acs5"
WINDOW_LABEL = f"{ACS_END_YEAR - 4}_{ACS_END_YEAR}"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet"

GEO_COLS = ["state_fips", "county_fips", "geo_id", "state_name", "county_name"]


def census_api_key() -> str:
    load_dotenv(PROJECT_ROOT / ".env")
    key = os.environ.get("CENSUS_API_KEY")
    if not key:
        sys.exit("CENSUS_API_KEY missing — set it in .env")
    return key


def fetch_acs5_county(get_vars: Iterable[str]) -> list:
    """Fetch the given ACS variables for all US counties from the 5-year ACS."""
    url = f"https://api.census.gov/data/{ACS_END_YEAR}/{ACS_DATASET}"
    params = {
        "get": ",".join(["NAME", *get_vars]),
        "for": "county:*",
        "key": census_api_key(),
    }
    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()


def fetch_acs5_county_group(table_id: str) -> list:
    """Fetch every variable in an ACS table for all US counties via the group() shortcut."""
    url = f"https://api.census.gov/data/{ACS_END_YEAR}/{ACS_DATASET}"
    params = {
        "get": f"NAME,group({table_id})",
        "for": "county:*",
        "key": census_api_key(),
    }
    resp = requests.get(url, params=params, timeout=120)
    resp.raise_for_status()
    return resp.json()


def to_geo_dataframe(rows: list) -> pd.DataFrame:
    """Header + data rows → DataFrame with standard geo columns added."""
    header, *data = rows
    df = pd.DataFrame(data, columns=header)

    # NAME comes back as "County Name, State Name"
    name_parts = df["NAME"].str.split(", ", n=1, expand=True)
    df["county_name"] = name_parts[0]
    df["state_name"] = name_parts[1]
    df["state_fips"] = df["state"]
    df["county_fips"] = df["county"]
    df["geo_id"] = df["state"] + df["county"]
    return df


def coerce_acs_ints(df: pd.DataFrame, rename: Dict[str, str]) -> pd.DataFrame:
    """Map ACS variable columns to friendly names as nullable Int64.
    Negative sentinels (-666666666 / -222222222 / -888888888) → null.
    """
    df = df.copy()
    for src, dst in rename.items():
        col = pd.to_numeric(df[src], errors="coerce")
        df[dst] = col.where(col >= 0).astype("Int64")
    return df


def coerce_acs_floats(df: pd.DataFrame, rename: Dict[str, str]) -> pd.DataFrame:
    """Same as coerce_acs_ints, but for fractional values (e.g. median age)."""
    df = df.copy()
    for src, dst in rename.items():
        col = pd.to_numeric(df[src], errors="coerce")
        df[dst] = col.where(col >= 0).astype("Float64")
    return df


def save(df: pd.DataFrame, table_id: str, raw_rows: list) -> Path:
    """Write Parquet (zstd) and a sibling raw JSON for reproducibility."""
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    raw_path = RAW_DIR / f"acs_{table_id.lower()}_county_{WINDOW_LABEL}.json"
    raw_path.write_text(json.dumps(raw_rows))

    parquet_path = PARQUET_DIR / f"acs_{table_id.lower()}_county_{WINDOW_LABEL}.parquet"
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        parquet_path,
        compression="zstd",
    )
    return parquet_path


def report(df: pd.DataFrame, parquet_path: Path, value_cols: Iterable[str]) -> None:
    print(f"  counties: {len(df)}")
    for col in value_cols:
        nulls = df[col].isna().sum()
        print(f"  {col}: {nulls} null")
    print(f"  parquet: {parquet_path.relative_to(PROJECT_ROOT)} ({parquet_path.stat().st_size:,} bytes)")

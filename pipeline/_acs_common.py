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
from typing import Dict, Iterable, List, NamedTuple, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dotenv import load_dotenv


class Bracket(NamedTuple):
    """One row in a long-format distribution.

    `suffixes` is the list of ACS variable id suffixes (e.g. ["003"] or
    ["002", "003"]) whose estimate columns are summed into this bracket's
    `units` count. `min`/`max` are the numeric range; null for open-ended
    or categorical entries.
    """
    suffixes: List[str]
    label: str
    min: Optional[int]
    max: Optional[int]

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
    """Header + data rows → DataFrame with standard geo columns added.

    The group() endpoint returns NAME at both the start and the end of the row,
    plus EA/MA annotation columns we don't need; drop duplicate columns here.
    """
    header, *data = rows
    df = pd.DataFrame(data, columns=header)
    df = df.loc[:, ~df.columns.duplicated()]

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


def write_long_format_brackets(
    table_id: str,
    brackets: List[Bracket],
    total_col_name: str,
    total_var_suffix: str = "001",
) -> Path:
    """Read banked raw JSON, sum bracket variables, write long-format Parquet.

    One row per (geo × bracket). Bracket entries can sum multiple raw
    variables (useful for grouped categorical tables like education).
    """
    raw_path = RAW_DIR / f"acs_{table_id.lower()}_county_{WINDOW_LABEL}.json"
    if not raw_path.exists():
        sys.exit(f"missing raw JSON: {raw_path}\nRun: python -m pipeline.bank_raw")

    rows = json.loads(raw_path.read_text())
    df = to_geo_dataframe(rows)

    rename = {f"{table_id}_{total_var_suffix}E": total_col_name}
    all_suffixes = sorted({s for b in brackets for s in b.suffixes})
    for suffix in all_suffixes:
        rename[f"{table_id}_{suffix}E"] = f"_units_{suffix}"
    df = coerce_acs_ints(df, rename)

    long_rows: list[dict] = []
    for idx, bracket in enumerate(brackets, start=1):
        for _, row in df.iterrows():
            parts = [row[f"_units_{s}"] for s in bracket.suffixes]
            valid = [p for p in parts if pd.notna(p)]
            units = int(sum(valid)) if len(valid) == len(parts) else None
            total = row[total_col_name]
            total_int = int(total) if pd.notna(total) else None
            share = (
                round(units / total_int, 6)
                if units is not None and total_int and total_int > 0
                else None
            )
            long_rows.append({
                "state_fips": row["state_fips"],
                "county_fips": row["county_fips"],
                "geo_id": row["geo_id"],
                "state_name": row["state_name"],
                "county_name": row["county_name"],
                total_col_name: total_int,
                "bracket_index": idx,
                "bracket_label": bracket.label,
                "bracket_min": bracket.min,
                "bracket_max": bracket.max,
                "units": units,
                "share": share,
            })

    long_df = pd.DataFrame(long_rows).sort_values(["geo_id", "bracket_index"]).reset_index(drop=True)

    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PARQUET_DIR / f"acs_{table_id.lower()}_county_{WINDOW_LABEL}.parquet"
    pq.write_table(
        pa.Table.from_pandas(long_df, preserve_index=False),
        out_path,
        compression="zstd",
    )

    n_counties = long_df["geo_id"].nunique()
    print(f"  rows: {len(long_df):,} ({n_counties} counties × {len(brackets)} brackets)")
    print(f"  parquet: {out_path.relative_to(PROJECT_ROOT)} ({out_path.stat().st_size:,} bytes)")
    return out_path

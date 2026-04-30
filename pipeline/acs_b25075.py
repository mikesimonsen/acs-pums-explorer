"""B25075 — Owner-occupied home value distribution (26 brackets).

Reads the raw JSON banked by `pipeline.bank_raw` (no Census API call here)
and emits a long-format Parquet with one row per (county × bracket).

Bracket labels and dollar ranges are Census-defined for the 2020-2024 5-year ACS.
The last bracket ($2M+) has no upper bound; bracket_max is null there.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline import _acs_common as c

TABLE = "B25075"

# (var_id_suffix, label, min_dollars, max_dollars or None for open-ended)
BRACKETS: List[Tuple[str, str, int, Optional[int]]] = [
    ("002", "Less than $10,000", 0, 9_999),
    ("003", "$10,000 to $14,999", 10_000, 14_999),
    ("004", "$15,000 to $19,999", 15_000, 19_999),
    ("005", "$20,000 to $24,999", 20_000, 24_999),
    ("006", "$25,000 to $29,999", 25_000, 29_999),
    ("007", "$30,000 to $34,999", 30_000, 34_999),
    ("008", "$35,000 to $39,999", 35_000, 39_999),
    ("009", "$40,000 to $49,999", 40_000, 49_999),
    ("010", "$50,000 to $59,999", 50_000, 59_999),
    ("011", "$60,000 to $69,999", 60_000, 69_999),
    ("012", "$70,000 to $79,999", 70_000, 79_999),
    ("013", "$80,000 to $89,999", 80_000, 89_999),
    ("014", "$90,000 to $99,999", 90_000, 99_999),
    ("015", "$100,000 to $124,999", 100_000, 124_999),
    ("016", "$125,000 to $149,999", 125_000, 149_999),
    ("017", "$150,000 to $174,999", 150_000, 174_999),
    ("018", "$175,000 to $199,999", 175_000, 199_999),
    ("019", "$200,000 to $249,999", 200_000, 249_999),
    ("020", "$250,000 to $299,999", 250_000, 299_999),
    ("021", "$300,000 to $399,999", 300_000, 399_999),
    ("022", "$400,000 to $499,999", 400_000, 499_999),
    ("023", "$500,000 to $749,999", 500_000, 749_999),
    ("024", "$750,000 to $999,999", 750_000, 999_999),
    ("025", "$1,000,000 to $1,499,999", 1_000_000, 1_499_999),
    ("026", "$1,500,000 to $1,999,999", 1_500_000, 1_999_999),
    ("027", "$2,000,000 or more", 2_000_000, None),
]

TOTAL_VAR = "B25075_001E"
RAW_PATH = c.RAW_DIR / f"acs_{TABLE.lower()}_county_{c.WINDOW_LABEL}.json"
OUT_PATH = c.PARQUET_DIR / f"acs_{TABLE.lower()}_county_{c.WINDOW_LABEL}.parquet"


def main() -> None:
    if not RAW_PATH.exists():
        sys.exit(f"missing raw JSON: {RAW_PATH}\nRun: python -m pipeline.bank_raw")

    rows = json.loads(RAW_PATH.read_text())
    df = c.to_geo_dataframe(rows)

    # Wide table → coerce all numeric variables we need to nullable Int64
    rename = {TOTAL_VAR: "total_owner_occupied"}
    for suffix, _, _, _ in BRACKETS:
        rename[f"{TABLE}_{suffix}E"] = f"units_{suffix}"
    df = c.coerce_acs_ints(df, rename)

    # Pivot to long format: one row per (county × bracket)
    long_rows: list[dict] = []
    for idx, (suffix, label, lo, hi) in enumerate(BRACKETS, start=1):
        col = f"units_{suffix}"
        for _, row in df.iterrows():
            units = row[col]
            total = row["total_owner_occupied"]
            share = (
                float(units) / float(total)
                if pd.notna(units) and pd.notna(total) and total > 0
                else None
            )
            long_rows.append({
                "state_fips": row["state_fips"],
                "county_fips": row["county_fips"],
                "geo_id": row["geo_id"],
                "state_name": row["state_name"],
                "county_name": row["county_name"],
                "total_owner_occupied": int(total) if pd.notna(total) else None,
                "bracket_index": idx,
                "bracket_label": label,
                "bracket_min": lo,
                "bracket_max": hi,
                "units": int(units) if pd.notna(units) else None,
                "share": round(share, 6) if share is not None else None,
            })

    long_df = pd.DataFrame(long_rows)
    long_df = long_df.sort_values(["geo_id", "bracket_index"]).reset_index(drop=True)

    c.PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(long_df, preserve_index=False),
        OUT_PATH,
        compression="zstd",
    )

    rel = OUT_PATH.relative_to(c.PROJECT_ROOT)
    print(f"  rows: {len(long_df):,} ({long_df['geo_id'].nunique()} counties × {len(BRACKETS)} brackets)")
    print(f"  parquet: {rel} ({OUT_PATH.stat().st_size:,} bytes)")


if __name__ == "__main__":
    main()

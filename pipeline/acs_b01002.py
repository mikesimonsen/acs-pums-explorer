"""B01002 — Median age (overall), county-level, 2020-2024 5-year ACS.

Note: B01002 also publishes median age by sex (B01002_002E male, B01002_003E female).
This script only pulls the overall median (B01002_001E). Add sex-disaggregated columns later if needed.
"""
from __future__ import annotations

from pipeline import _acs_common as c

TABLE = "B01002"
RENAME = {
    "B01002_001E": "median_age",
    "B01002_001M": "median_age_moe",
}


def main() -> None:
    print(f"Fetching {TABLE} from {c.ACS_END_YEAR - 4}-{c.ACS_END_YEAR} 5-year ACS...")
    rows = c.fetch_acs5_county(RENAME.keys())
    df = c.to_geo_dataframe(rows)
    df = c.coerce_acs_floats(df, RENAME)

    out = df[c.GEO_COLS + list(RENAME.values())].sort_values("geo_id").reset_index(drop=True)
    parquet_path = c.save(out, TABLE, rows)
    c.report(out, parquet_path, ["median_age"])


if __name__ == "__main__":
    main()

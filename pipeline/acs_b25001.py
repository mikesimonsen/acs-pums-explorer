"""B25001 — Total housing units (count of all housing units, occupied + vacant)."""
from __future__ import annotations

from pipeline import _acs_common as c

TABLE = "B25001"
RENAME = {
    "B25001_001E": "total_housing_units",
    "B25001_001M": "total_housing_units_moe",
}


def main() -> None:
    print(f"Fetching {TABLE} from {c.ACS_END_YEAR - 4}-{c.ACS_END_YEAR} 5-year ACS...")
    rows = c.fetch_acs5_county(RENAME.keys())
    df = c.to_geo_dataframe(rows)
    df = c.coerce_acs_ints(df, RENAME)

    out = df[c.GEO_COLS + list(RENAME.values())].sort_values("geo_id").reset_index(drop=True)
    parquet_path = c.save(out, TABLE, rows)
    c.report(out, parquet_path, ["total_housing_units"])


if __name__ == "__main__":
    main()

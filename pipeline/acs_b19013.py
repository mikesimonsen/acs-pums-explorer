"""B19013 — Median household income (dollars), county-level, 2020-2024 5-year ACS."""
from __future__ import annotations

from pipeline import _acs_common as c

TABLE = "B19013"
RENAME = {
    "B19013_001E": "median_household_income",
    "B19013_001M": "median_household_income_moe",
}


def main() -> None:
    print(f"Fetching {TABLE} from {c.ACS_END_YEAR - 4}-{c.ACS_END_YEAR} 5-year ACS...")
    rows = c.fetch_acs5_county(RENAME.keys())
    df = c.to_geo_dataframe(rows)
    df = c.coerce_acs_ints(df, RENAME)

    out = df[c.GEO_COLS + list(RENAME.values())].sort_values("geo_id").reset_index(drop=True)
    parquet_path = c.save(out, TABLE, rows)
    c.report(out, parquet_path, ["median_household_income"])


if __name__ == "__main__":
    main()

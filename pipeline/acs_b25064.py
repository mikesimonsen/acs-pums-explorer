"""B25064 — Median gross rent (dollars), county-level, 2020-2024 5-year ACS."""
from __future__ import annotations

from pipeline import _acs_common as c

TABLE = "B25064"
RENAME = {
    "B25064_001E": "median_gross_rent",
    "B25064_001M": "median_gross_rent_moe",
}


def main() -> None:
    print(f"Fetching {TABLE} from {c.ACS_END_YEAR - 4}-{c.ACS_END_YEAR} 5-year ACS...")
    rows = c.fetch_acs5_county(RENAME.keys())
    df = c.to_geo_dataframe(rows)
    df = c.coerce_acs_ints(df, RENAME)

    out = df[c.GEO_COLS + list(RENAME.values())].sort_values("geo_id").reset_index(drop=True)
    parquet_path = c.save(out, TABLE, rows)
    c.report(out, parquet_path, ["median_gross_rent"])


if __name__ == "__main__":
    main()

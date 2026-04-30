"""B25002 — Occupancy status (total/occupied/vacant) with derived vacancy_rate."""
from __future__ import annotations

from pipeline import _acs_common as c

TABLE = "B25002"
RENAME = {
    "B25002_001E": "total_housing_units",
    "B25002_001M": "total_housing_units_moe",
    "B25002_002E": "occupied_units",
    "B25002_002M": "occupied_units_moe",
    "B25002_003E": "vacant_units",
    "B25002_003M": "vacant_units_moe",
}
VALUE_COLS = ["total_housing_units", "occupied_units", "vacant_units", "vacancy_rate"]


def main() -> None:
    print(f"Fetching {TABLE} from {c.ACS_END_YEAR - 4}-{c.ACS_END_YEAR} 5-year ACS...")
    rows = c.fetch_acs5_county(RENAME.keys())
    df = c.to_geo_dataframe(rows)
    df = c.coerce_acs_ints(df, RENAME)

    rate = df["vacant_units"].astype("Float64") / df["total_housing_units"].astype("Float64")
    df["vacancy_rate"] = rate.round(4)

    out = df[c.GEO_COLS + list(RENAME.values()) + ["vacancy_rate"]].sort_values("geo_id").reset_index(drop=True)
    parquet_path = c.save(out, TABLE, rows)
    c.report(out, parquet_path, VALUE_COLS)


if __name__ == "__main__":
    main()

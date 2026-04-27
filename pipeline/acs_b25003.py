"""B25003 — Tenure (occupied housing units by owner/renter), county-level, 2020-2024 5-year ACS.

Adds a derived homeownership_rate column (owner / total).
"""
from __future__ import annotations

from pipeline import _acs_common as c

TABLE = "B25003"
RENAME = {
    "B25003_001E": "occupied_units",
    "B25003_001M": "occupied_units_moe",
    "B25003_002E": "owner_occupied_units",
    "B25003_002M": "owner_occupied_units_moe",
    "B25003_003E": "renter_occupied_units",
    "B25003_003M": "renter_occupied_units_moe",
}
VALUE_COLS = [
    "occupied_units",
    "owner_occupied_units",
    "renter_occupied_units",
    "homeownership_rate",
]


def main() -> None:
    print(f"Fetching {TABLE} from {c.ACS_END_YEAR - 4}-{c.ACS_END_YEAR} 5-year ACS...")
    rows = c.fetch_acs5_county(RENAME.keys())
    df = c.to_geo_dataframe(rows)
    df = c.coerce_acs_ints(df, RENAME)

    rate = df["owner_occupied_units"].astype("Float64") / df["occupied_units"].astype("Float64")
    df["homeownership_rate"] = rate.round(4)

    out = df[c.GEO_COLS + list(RENAME.values()) + ["homeownership_rate"]].sort_values("geo_id").reset_index(drop=True)
    parquet_path = c.save(out, TABLE, rows)
    c.report(out, parquet_path, VALUE_COLS)


if __name__ == "__main__":
    main()

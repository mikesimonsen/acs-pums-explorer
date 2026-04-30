"""Bank raw JSON for ACS tables we'll convert to Parquet later (offline).

Each table is fetched once via the group() shortcut and dumped to data/raw/.
Skips tables whose JSON is already present locally.
"""
from __future__ import annotations

import json
import sys

from pipeline import _acs_common as c

TABLES_TO_BANK = [
    # Tier 2 — bracketed distributions
    ("B25075", "Home value brackets (owner-occupied)"),
    ("B19001", "Household income brackets"),
    ("B25034", "Year structure built"),
    ("B25024", "Units in structure"),
    ("B25041", "Bedrooms"),
    # Tier 3 — categorical breakdowns
    ("B03002", "Hispanic or Latino origin by race"),
    ("B15003", "Educational attainment (population 25+)"),
    ("B11016", "Household type by household size"),
    ("B23025", "Employment status"),
]


def main() -> None:
    c.RAW_DIR.mkdir(parents=True, exist_ok=True)
    failures: list[str] = []

    for table_id, desc in TABLES_TO_BANK:
        out_path = c.RAW_DIR / f"acs_{table_id.lower()}_county_{c.WINDOW_LABEL}.json"
        if out_path.exists():
            print(f"  skip {table_id} — already banked ({out_path.stat().st_size:,} bytes)")
            continue
        print(f"Fetching {table_id} ({desc})...")
        try:
            rows = c.fetch_acs5_county_group(table_id)
        except Exception as e:
            print(f"  ERROR {table_id}: {e}", file=sys.stderr)
            failures.append(table_id)
            continue
        out_path.write_text(json.dumps(rows))
        # First row = header. Variable count = header length minus NAME/state/county = 3.
        n_vars = max(0, len(rows[0]) - 3)
        print(f"  saved {out_path.relative_to(c.PROJECT_ROOT)} — {len(rows) - 1} rows, {n_vars} variables, {out_path.stat().st_size:,} bytes")

    if failures:
        print(f"\nFAILED: {failures}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

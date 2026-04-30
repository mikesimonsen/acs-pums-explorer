"""B25041 — Bedrooms per housing unit (6 buckets, 0 → 5+)."""
from __future__ import annotations

from pipeline._acs_common import Bracket, write_long_format_brackets

TABLE = "B25041"

BRACKETS = [
    Bracket(["002"], "No bedroom (studio)", 0, 0),
    Bracket(["003"], "1 bedroom", 1, 1),
    Bracket(["004"], "2 bedrooms", 2, 2),
    Bracket(["005"], "3 bedrooms", 3, 3),
    Bracket(["006"], "4 bedrooms", 4, 4),
    Bracket(["007"], "5 or more bedrooms", 5, None),
]


def main() -> None:
    write_long_format_brackets(TABLE, BRACKETS, total_col_name="total_housing_units")


if __name__ == "__main__":
    main()

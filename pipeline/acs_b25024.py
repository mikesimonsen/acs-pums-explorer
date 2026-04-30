"""B25024 — Units in structure (housing type taxonomy, 10 categories)."""
from __future__ import annotations

from pipeline._acs_common import Bracket, write_long_format_brackets

TABLE = "B25024"

BRACKETS = [
    Bracket(["002"], "Single-family detached", None, None),
    Bracket(["003"], "Single-family attached", None, None),
    Bracket(["004"], "Duplex (2 units)", None, None),
    Bracket(["005"], "3 or 4 units", None, None),
    Bracket(["006"], "5 to 9 units", None, None),
    Bracket(["007"], "10 to 19 units", None, None),
    Bracket(["008"], "20 to 49 units", None, None),
    Bracket(["009"], "50 or more units", None, None),
    Bracket(["010"], "Mobile home", None, None),
    Bracket(["011"], "Boat, RV, van, etc.", None, None),
]


def main() -> None:
    write_long_format_brackets(TABLE, BRACKETS, total_col_name="total_housing_units")


if __name__ == "__main__":
    main()

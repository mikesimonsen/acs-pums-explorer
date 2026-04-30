"""B25034 — Year structure built (10 buckets, oldest to newest)."""
from __future__ import annotations

from pipeline._acs_common import Bracket, write_long_format_brackets

TABLE = "B25034"

# Reversed from Census order so the histogram reads left-to-right oldest → newest.
BRACKETS = [
    Bracket(["011"], "Built 1939 or earlier", None, 1939),
    Bracket(["010"], "Built 1940 to 1949", 1940, 1949),
    Bracket(["009"], "Built 1950 to 1959", 1950, 1959),
    Bracket(["008"], "Built 1960 to 1969", 1960, 1969),
    Bracket(["007"], "Built 1970 to 1979", 1970, 1979),
    Bracket(["006"], "Built 1980 to 1989", 1980, 1989),
    Bracket(["005"], "Built 1990 to 1999", 1990, 1999),
    Bracket(["004"], "Built 2000 to 2009", 2000, 2009),
    Bracket(["003"], "Built 2010 to 2019", 2010, 2019),
    Bracket(["002"], "Built 2020 or later", 2020, None),
]


def main() -> None:
    write_long_format_brackets(TABLE, BRACKETS, total_col_name="total_housing_units")


if __name__ == "__main__":
    main()

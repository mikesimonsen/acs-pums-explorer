"""B03002 — Hispanic or Latino origin by race.

Categorical breakdown using 8 mutually-exclusive groups that sum to total
population: 7 non-Hispanic races + Hispanic of any race.
"""
from __future__ import annotations

from pipeline._acs_common import Bracket, write_long_format_brackets

TABLE = "B03002"

BRACKETS = [
    Bracket(["003"], "White, non-Hispanic", None, None),
    Bracket(["004"], "Black or African American, non-Hispanic", None, None),
    Bracket(["006"], "Asian, non-Hispanic", None, None),
    Bracket(["005"], "American Indian / Alaska Native, non-Hispanic", None, None),
    Bracket(["007"], "Native Hawaiian / Pacific Islander, non-Hispanic", None, None),
    Bracket(["008"], "Some other race, non-Hispanic", None, None),
    Bracket(["009"], "Two or more races, non-Hispanic", None, None),
    Bracket(["012"], "Hispanic or Latino (any race)", None, None),
]


def main() -> None:
    write_long_format_brackets(TABLE, BRACKETS, total_col_name="total_population")


if __name__ == "__main__":
    main()

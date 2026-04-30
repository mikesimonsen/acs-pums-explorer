"""B15003 — Educational attainment for the population age 25+.

The raw table has 24 fine-grained categories. This script collapses them
to the 5-bucket grouping commonly used in summary visualizations.
"""
from __future__ import annotations

from pipeline._acs_common import Bracket, write_long_format_brackets

TABLE = "B15003"

# Suffixes 002-016 = no schooling through "12th grade, no diploma" (15 categories).
LESS_THAN_HS = [f"{n:03d}" for n in range(2, 17)]

BRACKETS = [
    Bracket(LESS_THAN_HS, "Less than high school", None, None),
    Bracket(["017", "018"], "High school graduate (incl. GED)", None, None),
    Bracket(["019", "020", "021"], "Some college / Associate's", None, None),
    Bracket(["022"], "Bachelor's degree", None, None),
    Bracket(["023", "024", "025"], "Graduate / professional degree", None, None),
]


def main() -> None:
    write_long_format_brackets(TABLE, BRACKETS, total_col_name="population_25_plus")


if __name__ == "__main__":
    main()

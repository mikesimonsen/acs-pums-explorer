"""B23025 — Employment status of the population age 16+ (4 mutually-exclusive groups)."""
from __future__ import annotations

from pipeline._acs_common import Bracket, write_long_format_brackets

TABLE = "B23025"

# Census ACS B23025 hierarchy:
#   001 = Total 16+
#   002 = In labor force
#     003 = Civilian labor force
#       004 = Employed
#       005 = Unemployed
#     006 = Armed Forces
#   007 = Not in labor force
# 004+005+006+007 sums to 001 (mutually exclusive).
BRACKETS = [
    Bracket(["004"], "Employed (civilian)", None, None),
    Bracket(["005"], "Unemployed (civilian)", None, None),
    Bracket(["006"], "Armed Forces", None, None),
    Bracket(["007"], "Not in labor force", None, None),
]


def main() -> None:
    write_long_format_brackets(TABLE, BRACKETS, total_col_name="population_16_plus")


if __name__ == "__main__":
    main()

"""B11016 — Household size (combines family + nonfamily into 7 size buckets)."""
from __future__ import annotations

from pipeline._acs_common import Bracket, write_long_format_brackets

TABLE = "B11016"

# B11016 splits households by family vs nonfamily, then by size within each.
# Sum across both to get total households at each size.
#   003-008 = family households, 2-person to 7+
#   010    = nonfamily 1-person (only nonfamilies can be 1-person)
#   011-016 = nonfamily households, 2-person to 7+
BRACKETS = [
    Bracket(["010"], "1 person", 1, 1),
    Bracket(["003", "011"], "2 people", 2, 2),
    Bracket(["004", "012"], "3 people", 3, 3),
    Bracket(["005", "013"], "4 people", 4, 4),
    Bracket(["006", "014"], "5 people", 5, 5),
    Bracket(["007", "015"], "6 people", 6, 6),
    Bracket(["008", "016"], "7 or more people", 7, None),
]


def main() -> None:
    write_long_format_brackets(TABLE, BRACKETS, total_col_name="total_households")


if __name__ == "__main__":
    main()

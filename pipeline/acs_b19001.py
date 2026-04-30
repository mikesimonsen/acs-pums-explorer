"""B19001 — Household income distribution (16 brackets)."""
from __future__ import annotations

from pipeline._acs_common import Bracket, write_long_format_brackets

TABLE = "B19001"

BRACKETS = [
    Bracket(["002"], "Less than $10,000", 0, 9_999),
    Bracket(["003"], "$10,000 to $14,999", 10_000, 14_999),
    Bracket(["004"], "$15,000 to $19,999", 15_000, 19_999),
    Bracket(["005"], "$20,000 to $24,999", 20_000, 24_999),
    Bracket(["006"], "$25,000 to $29,999", 25_000, 29_999),
    Bracket(["007"], "$30,000 to $34,999", 30_000, 34_999),
    Bracket(["008"], "$35,000 to $39,999", 35_000, 39_999),
    Bracket(["009"], "$40,000 to $44,999", 40_000, 44_999),
    Bracket(["010"], "$45,000 to $49,999", 45_000, 49_999),
    Bracket(["011"], "$50,000 to $59,999", 50_000, 59_999),
    Bracket(["012"], "$60,000 to $74,999", 60_000, 74_999),
    Bracket(["013"], "$75,000 to $99,999", 75_000, 99_999),
    Bracket(["014"], "$100,000 to $124,999", 100_000, 124_999),
    Bracket(["015"], "$125,000 to $149,999", 125_000, 149_999),
    Bracket(["016"], "$150,000 to $199,999", 150_000, 199_999),
    Bracket(["017"], "$200,000 or more", 200_000, None),
]


def main() -> None:
    write_long_format_brackets(TABLE, BRACKETS, total_col_name="total_households")


if __name__ == "__main__":
    main()

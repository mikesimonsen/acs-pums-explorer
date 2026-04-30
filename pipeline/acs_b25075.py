"""B25075 — Owner-occupied home value distribution (26 brackets).

Reads the raw JSON banked by `pipeline.bank_raw` and emits a long-format
Parquet with one row per (county × bracket).
"""
from __future__ import annotations

from pipeline._acs_common import Bracket, write_long_format_brackets

TABLE = "B25075"

BRACKETS = [
    Bracket(["002"], "Less than $10,000", 0, 9_999),
    Bracket(["003"], "$10,000 to $14,999", 10_000, 14_999),
    Bracket(["004"], "$15,000 to $19,999", 15_000, 19_999),
    Bracket(["005"], "$20,000 to $24,999", 20_000, 24_999),
    Bracket(["006"], "$25,000 to $29,999", 25_000, 29_999),
    Bracket(["007"], "$30,000 to $34,999", 30_000, 34_999),
    Bracket(["008"], "$35,000 to $39,999", 35_000, 39_999),
    Bracket(["009"], "$40,000 to $49,999", 40_000, 49_999),
    Bracket(["010"], "$50,000 to $59,999", 50_000, 59_999),
    Bracket(["011"], "$60,000 to $69,999", 60_000, 69_999),
    Bracket(["012"], "$70,000 to $79,999", 70_000, 79_999),
    Bracket(["013"], "$80,000 to $89,999", 80_000, 89_999),
    Bracket(["014"], "$90,000 to $99,999", 90_000, 99_999),
    Bracket(["015"], "$100,000 to $124,999", 100_000, 124_999),
    Bracket(["016"], "$125,000 to $149,999", 125_000, 149_999),
    Bracket(["017"], "$150,000 to $174,999", 150_000, 174_999),
    Bracket(["018"], "$175,000 to $199,999", 175_000, 199_999),
    Bracket(["019"], "$200,000 to $249,999", 200_000, 249_999),
    Bracket(["020"], "$250,000 to $299,999", 250_000, 299_999),
    Bracket(["021"], "$300,000 to $399,999", 300_000, 399_999),
    Bracket(["022"], "$400,000 to $499,999", 400_000, 499_999),
    Bracket(["023"], "$500,000 to $749,999", 500_000, 749_999),
    Bracket(["024"], "$750,000 to $999,999", 750_000, 999_999),
    Bracket(["025"], "$1,000,000 to $1,499,999", 1_000_000, 1_499_999),
    Bracket(["026"], "$1,500,000 to $1,999,999", 1_500_000, 1_999_999),
    Bracket(["027"], "$2,000,000 or more", 2_000_000, None),
]


def main() -> None:
    write_long_format_brackets(TABLE, BRACKETS, total_col_name="total_owner_occupied")


if __name__ == "__main__":
    main()

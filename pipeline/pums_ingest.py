"""PUMS 2024 1-year ingestion: per-state housing + person records.

Downloads each requested state's housing and person ZIPs from
www2.census.gov, extracts the CSV inside, keeps only the columns we
need, and writes per-state Parquet files.

Per-state outputs land at:
    data/parquet/pums/h_{state_fips}.parquet
    data/parquet/pums/p_{state_fips}.parquet

Raw ZIPs are cached under data/raw/pums/. Re-running skips states whose
parquets already exist; pass --force to re-download.

Usage:
    python -m pipeline.pums_ingest 56              # Wyoming only
    python -m pipeline.pums_ingest 06 36 48        # CA, NY, TX
    python -m pipeline.pums_ingest --all           # 50 states + DC
"""
from __future__ import annotations

import argparse
import sys
import time
import zipfile
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "pums"
PARQUET_DIR = PROJECT_ROOT / "data" / "parquet" / "pums"

PUMS_YEAR = 2024
PUMS_BASE = f"https://www2.census.gov/programs-surveys/acs/data/pums/{PUMS_YEAR}/1-Year"

# Note: 2024 PUMS renamed "ST" to "STATE" and "YBL" to "YRBLT".
HOUSING_COLS = [
    "SERIALNO", "STATE", "PUMA", "WGTP",
    "TEN", "VALP", "RNTP", "GRPIP", "OCPIP",
    "HINCP", "FINCP", "MV", "BDSP", "RMSP", "YRBLT",
    "HHL",  # Household language: 1=English only, 2=Spanish, 3=Other Indo-Eur, 4=Asian/PI, 5=Other
]

PERSON_COLS = [
    "SERIALNO", "SPORDER", "STATE", "PUMA", "PWGTP",
    "AGEP", "RAC1P", "HISP", "SCHL",
    "SEX",  # 1=Male, 2=Female
    "CIT",  # Citizenship status (1-5)
    "DIS",  # Disability recode: 1=With, 2=Without
]

STRING_COLS = {"SERIALNO", "STATE", "PUMA"}

# State FIPS → 2-letter postal (lowercased for Census file naming).
# DC included; PR excluded — PR PUMS lives in a different dataset.
STATE_FIPS_TO_POSTAL = {
    "01": "al", "02": "ak", "04": "az", "05": "ar", "06": "ca",
    "08": "co", "09": "ct", "10": "de", "11": "dc", "12": "fl",
    "13": "ga", "15": "hi", "16": "id", "17": "il", "18": "in",
    "19": "ia", "20": "ks", "21": "ky", "22": "la", "23": "me",
    "24": "md", "25": "ma", "26": "mi", "27": "mn", "28": "ms",
    "29": "mo", "30": "mt", "31": "ne", "32": "nv", "33": "nh",
    "34": "nj", "35": "nm", "36": "ny", "37": "nc", "38": "nd",
    "39": "oh", "40": "ok", "41": "or", "42": "pa", "44": "ri",
    "45": "sc", "46": "sd", "47": "tn", "48": "tx", "49": "ut",
    "50": "vt", "51": "va", "53": "wa", "54": "wv", "55": "wi",
    "56": "wy",
}


def download_zip(url: str, dest: Path, timeout: int = 600, max_attempts: int = 4) -> Path:
    """Stream-download URL to dest, atomic via .tmp rename. Skips if dest exists.

    Cloudflare returns 520/524 transiently for large PUMS files; retry with
    exponential backoff (5s, 15s, 45s) and treat connection drops the same way.
    """
    if dest.exists():
        print(f"  zip cached: {dest.name} ({dest.stat().st_size:,} bytes)")
        return dest
    print(f"  GET {url}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")

    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.get(url, timeout=timeout, stream=True)
            resp.raise_for_status()
            bytes_written = 0
            with tmp.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    if not chunk:
                        continue
                    f.write(chunk)
                    bytes_written += len(chunk)
            tmp.rename(dest)
            print(f"  saved {dest.name} ({bytes_written:,} bytes)")
            return dest
        except (requests.exceptions.RequestException, requests.exceptions.ChunkedEncodingError) as e:
            last_err = e
            if tmp.exists():
                tmp.unlink()
            if attempt == max_attempts:
                break
            wait = 5 * (3 ** (attempt - 1))
            print(f"  attempt {attempt}/{max_attempts} failed ({type(e).__name__}); retrying in {wait}s")
            time.sleep(wait)
    raise last_err if last_err else RuntimeError(f"download failed: {url}")


def csv_to_parquet(zip_path: Path, parquet_path: Path, wanted_cols: List[str]) -> int:
    """Read the psam CSV inside the ZIP, project + coerce columns, write Parquet."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().startswith("psam_") and n.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"no psam_*.csv in {zip_path.name}")
        # Read everything as string so we can do explicit numeric coercion.
        with zf.open(csv_names[0]) as f:
            df = pd.read_csv(f, dtype=str, low_memory=False)

    missing = set(wanted_cols) - set(df.columns)
    if missing:
        print(f"  WARN missing columns: {sorted(missing)}", file=sys.stderr)
    keep = [c for c in wanted_cols if c in df.columns]
    df = df[keep].copy()

    for col in df.columns:
        if col in STRING_COLS:
            s = df[col].astype(str)
            if col == "STATE":
                s = s.str.zfill(2)
            elif col == "PUMA":
                s = s.str.zfill(5)
            df[col] = s
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        parquet_path,
        compression="zstd",
    )
    return len(df)


def ingest_state(state_fips: str, force: bool = False) -> bool:
    postal = STATE_FIPS_TO_POSTAL.get(state_fips)
    if not postal:
        print(f"  unknown state FIPS: {state_fips}", file=sys.stderr)
        return False

    print(f"=== {state_fips} ({postal.upper()}) ===")
    ok = True
    for record_type, cols, label in [
        ("h", HOUSING_COLS, "housing"),
        ("p", PERSON_COLS, "person "),
    ]:
        zip_url = f"{PUMS_BASE}/csv_{record_type}{postal}.zip"
        zip_path = RAW_DIR / f"csv_{record_type}{postal}.zip"
        parquet_path = PARQUET_DIR / f"{record_type}_{state_fips}.parquet"

        if parquet_path.exists() and not force:
            print(f"  {label}: parquet exists ({parquet_path.stat().st_size:,} bytes)")
            continue

        try:
            download_zip(zip_url, zip_path)
            n_rows = csv_to_parquet(zip_path, parquet_path, cols)
            size = parquet_path.stat().st_size
            print(f"  {label}: {n_rows:,} rows → {parquet_path.relative_to(PROJECT_ROOT)} ({size:,} bytes)")
        except Exception as e:
            print(f"  ERROR {label.strip()}: {type(e).__name__}: {e}", file=sys.stderr)
            ok = False
    return ok


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("states", nargs="*", help="State FIPS codes")
    parser.add_argument("--all", action="store_true", help="All 50 states + DC")
    parser.add_argument("--force", action="store_true", help="Re-download/rebuild")
    args = parser.parse_args(argv)

    if args.all:
        states = sorted(STATE_FIPS_TO_POSTAL.keys())
    elif args.states:
        states = args.states
    else:
        parser.error("specify state FIPS codes or --all")

    failures = []
    for state_fips in states:
        if not ingest_state(state_fips, args.force):
            failures.append(state_fips)
        print()

    if failures:
        print(f"FAILED: {failures}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

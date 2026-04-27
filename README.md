# acs-pums

Static-site app for exploring ACS and PUMS census data. Python ingestion → Parquet → DuckDB-WASM in the browser → Observable Plot. Hosted on GitHub Pages, no backend.

## Layout

```
pipeline/      Python ingestion scripts (ACS API, PUMS bulk files)
data/raw/      Raw downloads (gitignored)
data/parquet/  Parquet outputs (committed, served as static assets)
web/           Vite + TypeScript + DuckDB-WASM + Observable Plot
notebooks/     Ad-hoc Jupyter analysis
```

## Prereqs

- Python 3.11+
- Node 20+
- A Census API key — request one at <https://api.census.gov/data/key_signup.html>, then put it in `.env`:
  ```
  CENSUS_API_KEY=your_key_here
  ```

## Quickstart

Pipeline:
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .
python -m pipeline.acs_b25077  # writes data/parquet/acs_b25077_county_2020_2024.parquet
```

Web:
```bash
cd web
npm install
npm run dev
```

## Workflow

Explore in the app → download CSV → finalize charts in Flourish for publishing.

## Storage notes

Parquet outputs live in `data/parquet/` and are committed to git so GitHub Pages can serve them as static assets. Milestone 1 outputs are small (well under 1 MB), so we use plain git — no LFS. Reassess if any single Parquet exceeds ~50 MB.

import * as Plot from '@observablehq/plot';
import type { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { getDuckDB } from './duckdb';

const PARQUET_URL = new URL(
  'data/parquet/acs_b25077_county_2020_2024.parquet',
  document.baseURI,
).href;

const stateSelect = document.getElementById('state') as HTMLSelectElement;
const downloadBtn = document.getElementById('download') as HTMLButtonElement;
const chartEl = document.getElementById('chart') as HTMLDivElement;
const statusEl = document.getElementById('status') as HTMLSpanElement;

type CountyRow = {
  state_fips: string;
  county_fips: string;
  geo_id: string;
  state_name: string;
  county_name: string;
  median_home_value: number | null;
  median_home_value_moe: number | null;
};

let currentRows: CountyRow[] = [];
let currentStateName = '';

async function main() {
  const db = await getDuckDB();
  const conn = await db.connect();

  await conn.query(
    `CREATE OR REPLACE VIEW b25077 AS SELECT * FROM read_parquet('${PARQUET_URL}')`,
  );

  const states = await conn.query(
    `SELECT DISTINCT state_fips, state_name
     FROM b25077
     ORDER BY state_name`,
  );
  for (const row of states.toArray() as { state_fips: string; state_name: string }[]) {
    const opt = document.createElement('option');
    opt.value = row.state_fips;
    opt.textContent = row.state_name;
    stateSelect.appendChild(opt);
  }

  stateSelect.addEventListener('change', () => renderState(conn, stateSelect.value));
  downloadBtn.addEventListener('click', downloadCsv);

  stateSelect.disabled = false;
  downloadBtn.disabled = false;
  await renderState(conn, stateSelect.value);
}

async function renderState(conn: AsyncDuckDBConnection, stateFips: string) {
  if (!/^\d{2}$/.test(stateFips)) throw new Error(`bad state_fips: ${stateFips}`);
  statusEl.textContent = 'Querying…';

  const result = await conn.query(`
    SELECT state_fips, county_fips, geo_id, state_name, county_name,
           CAST(median_home_value AS INTEGER) AS median_home_value,
           CAST(median_home_value_moe AS INTEGER) AS median_home_value_moe
    FROM b25077
    WHERE state_fips = '${stateFips}'
    ORDER BY median_home_value DESC NULLS LAST
  `);
  currentRows = result.toArray() as CountyRow[];
  currentStateName = currentRows[0]?.state_name ?? '';

  chartEl.innerHTML = '';
  const plotted = currentRows.filter((r) => r.median_home_value != null);
  if (plotted.length === 0) {
    chartEl.textContent = 'No data for this state.';
    statusEl.textContent = `${currentRows.length} counties (all null)`;
    return;
  }

  const chart = Plot.plot({
    marginLeft: 200,
    marginRight: 60,
    height: Math.max(280, plotted.length * 20 + 60),
    x: { label: 'Median home value ($)', grid: true, tickFormat: '$~s' },
    y: { label: null },
    marks: [
      Plot.barX(plotted, {
        x: 'median_home_value',
        y: 'county_name',
        sort: { y: 'x', reverse: true },
        fill: '#4c78a8',
      }),
      Plot.text(plotted, {
        x: 'median_home_value',
        y: 'county_name',
        text: (d: CountyRow) =>
          d.median_home_value != null ? `$${(d.median_home_value / 1000).toFixed(0)}k` : '',
        textAnchor: 'start',
        dx: 4,
        fontSize: 10,
        fill: '#333',
      }),
      Plot.ruleX([0]),
    ],
  });
  chartEl.appendChild(chart);

  statusEl.textContent = `${currentRows.length} counties (${currentRows.length - plotted.length} null)`;
}

function downloadCsv() {
  if (!currentRows.length) return;
  const headers: (keyof CountyRow)[] = [
    'state_fips', 'county_fips', 'geo_id',
    'state_name', 'county_name',
    'median_home_value', 'median_home_value_moe',
  ];
  const lines = [headers.join(',')];
  for (const r of currentRows) {
    lines.push(headers.map((h) => csvCell(r[h])).join(','));
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `b25077_${currentStateName.replace(/\s+/g, '_').toLowerCase()}_2020_2024.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function csvCell(val: unknown): string {
  if (val == null) return '';
  const s = String(val);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

main().catch((err) => {
  statusEl.textContent = `Error: ${err.message}`;
  console.error(err);
});

import * as Plot from '@observablehq/plot';
import type { AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';
import { getDuckDB } from './duckdb';

type Metric = {
  key: string;
  label: string;
  table: string;
  parquet: string;
  valueCol: string;
  axisFormat: string;
  rowFormat: (v: number) => string;
};

const METRICS: Metric[] = [
  {
    key: 'home_value',
    label: 'Median home value',
    table: 'B25077',
    parquet: 'data/parquet/acs_b25077_county_2020_2024.parquet',
    valueCol: 'median_home_value',
    axisFormat: '$~s',
    rowFormat: (v) => `$${(v / 1000).toFixed(0)}k`,
  },
  {
    key: 'rent',
    label: 'Median gross rent',
    table: 'B25064',
    parquet: 'data/parquet/acs_b25064_county_2020_2024.parquet',
    valueCol: 'median_gross_rent',
    axisFormat: '$,d',
    rowFormat: (v) => `$${v.toLocaleString()}`,
  },
  {
    key: 'income',
    label: 'Median household income',
    table: 'B19013',
    parquet: 'data/parquet/acs_b19013_county_2020_2024.parquet',
    valueCol: 'median_household_income',
    axisFormat: '$~s',
    rowFormat: (v) => `$${(v / 1000).toFixed(0)}k`,
  },
  {
    key: 'ownership',
    label: 'Homeownership rate',
    table: 'B25003',
    parquet: 'data/parquet/acs_b25003_county_2020_2024.parquet',
    valueCol: 'homeownership_rate',
    axisFormat: '.0%',
    rowFormat: (v) => `${(v * 100).toFixed(1)}%`,
  },
];

const metricSelect = document.getElementById('metric') as HTMLSelectElement;
const stateSelect = document.getElementById('state') as HTMLSelectElement;
const downloadBtn = document.getElementById('download') as HTMLButtonElement;
const chartEl = document.getElementById('chart') as HTMLDivElement;
const statusEl = document.getElementById('status') as HTMLSpanElement;
const sourceEl = document.getElementById('source') as HTMLParagraphElement;

type Row = Record<string, unknown>;
let currentRows: Row[] = [];
let currentMetric: Metric = METRICS[0];
let currentStateName = '';

function url(rel: string): string {
  return new URL(rel, document.baseURI).href;
}

async function main() {
  const db = await getDuckDB();
  const conn = await db.connect();

  for (const m of METRICS) {
    const opt = document.createElement('option');
    opt.value = m.key;
    opt.textContent = m.label;
    metricSelect.appendChild(opt);
  }

  const states = await conn.query(`
    SELECT DISTINCT state_fips, state_name
    FROM read_parquet('${url(METRICS[0].parquet)}')
    ORDER BY state_name
  `);
  for (const row of states.toArray() as { state_fips: string; state_name: string }[]) {
    const opt = document.createElement('option');
    opt.value = row.state_fips;
    opt.textContent = row.state_name;
    stateSelect.appendChild(opt);
  }

  metricSelect.addEventListener('change', () => render(conn));
  stateSelect.addEventListener('change', () => render(conn));
  downloadBtn.addEventListener('click', downloadCsv);

  metricSelect.disabled = false;
  stateSelect.disabled = false;
  downloadBtn.disabled = false;
  await render(conn);
}

async function render(conn: AsyncDuckDBConnection) {
  const metric = METRICS.find((m) => m.key === metricSelect.value) ?? METRICS[0];
  const stateFips = stateSelect.value;
  if (!/^\d{2}$/.test(stateFips)) throw new Error(`bad state_fips: ${stateFips}`);

  currentMetric = metric;
  statusEl.textContent = 'Querying…';

  const result = await conn.query(`
    SELECT * REPLACE (CAST(${metric.valueCol} AS DOUBLE) AS ${metric.valueCol})
    FROM read_parquet('${url(metric.parquet)}')
    WHERE state_fips = '${stateFips}'
    ORDER BY ${metric.valueCol} DESC NULLS LAST
  `);
  currentRows = result.toArray() as Row[];
  currentStateName = (currentRows[0]?.state_name as string) ?? '';

  sourceEl.textContent = `Source: ACS table ${metric.table}, 5-year (${currentStateName}).`;

  chartEl.innerHTML = '';
  const plotted = currentRows.filter((r) => r[metric.valueCol] != null);
  if (plotted.length === 0) {
    chartEl.textContent = 'No data for this state.';
    statusEl.textContent = `${currentRows.length} counties (all null)`;
    return;
  }

  const chart = Plot.plot({
    marginLeft: 200,
    marginRight: 90,
    height: Math.max(280, plotted.length * 20 + 60),
    x: { label: metric.label, grid: true, tickFormat: metric.axisFormat },
    y: { label: null },
    marks: [
      Plot.barX(plotted, {
        x: metric.valueCol,
        y: 'county_name',
        sort: { y: 'x', reverse: true },
        fill: '#4c78a8',
      }),
      Plot.text(plotted, {
        x: metric.valueCol,
        y: 'county_name',
        text: (d: Row) => metric.rowFormat(d[metric.valueCol] as number),
        textAnchor: 'start',
        dx: 4,
        fontSize: 10,
        fill: '#333',
      }),
      Plot.ruleX([0]),
    ],
  });
  chartEl.appendChild(chart);

  const nulls = currentRows.length - plotted.length;
  statusEl.textContent = `${currentRows.length} counties${nulls ? ` (${nulls} null)` : ''}`;
}

function downloadCsv() {
  if (!currentRows.length) return;
  const headers = Object.keys(currentRows[0]);
  const lines = [headers.join(',')];
  for (const r of currentRows) {
    lines.push(headers.map((h) => csvCell(r[h])).join(','));
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
  const u = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = u;
  const stateSlug = currentStateName.replace(/\s+/g, '_').toLowerCase();
  a.download = `${currentMetric.table.toLowerCase()}_${stateSlug}_2020_2024.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(u);
}

function csvCell(val: unknown): string {
  if (val == null) return '';
  const s = typeof val === 'bigint' ? val.toString() : String(val);
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

main().catch((err) => {
  statusEl.textContent = `Error: ${err.message}`;
  console.error(err);
});

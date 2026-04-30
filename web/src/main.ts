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
  {
    key: 'housing_units',
    label: 'Total housing units',
    table: 'B25001',
    parquet: 'data/parquet/acs_b25001_county_2020_2024.parquet',
    valueCol: 'total_housing_units',
    axisFormat: '~s',
    rowFormat: (v) =>
      v >= 1e6 ? `${(v / 1e6).toFixed(2)}M` : v >= 1e3 ? `${(v / 1e3).toFixed(0)}k` : v.toLocaleString(),
  },
  {
    key: 'vacancy_rate',
    label: 'Vacancy rate',
    table: 'B25002',
    parquet: 'data/parquet/acs_b25002_county_2020_2024.parquet',
    valueCol: 'vacancy_rate',
    axisFormat: '.0%',
    rowFormat: (v) => `${(v * 100).toFixed(1)}%`,
  },
  {
    key: 'population',
    label: 'Total population',
    table: 'B01003',
    parquet: 'data/parquet/acs_b01003_county_2020_2024.parquet',
    valueCol: 'total_population',
    axisFormat: '~s',
    rowFormat: (v) =>
      v >= 1e6 ? `${(v / 1e6).toFixed(2)}M` : v >= 1e3 ? `${(v / 1e3).toFixed(0)}k` : v.toLocaleString(),
  },
  {
    key: 'median_age',
    label: 'Median age',
    table: 'B01002',
    parquet: 'data/parquet/acs_b01002_county_2020_2024.parquet',
    valueCol: 'median_age',
    axisFormat: '.0f',
    rowFormat: (v) => v.toFixed(1),
  },
];

const ALL_STATES_VALUE = 'all';
const TOP_N_NATIONWIDE = 50;

const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const viewSelect = $<HTMLSelectElement>('view');
const metricSelect = $<HTMLSelectElement>('metric');
const xMetricSelect = $<HTMLSelectElement>('xMetric');
const yMetricSelect = $<HTMLSelectElement>('yMetric');
const stateSelect = $<HTMLSelectElement>('state');
const downloadBtn = $<HTMLButtonElement>('download');
const chartEl = $<HTMLDivElement>('chart');
const statusEl = $<HTMLSpanElement>('status');
const sourceEl = $<HTMLParagraphElement>('source');

type Row = Record<string, unknown>;
let currentRows: Row[] = [];
let currentCsvHeaders: string[] = [];
let currentCsvFilename = 'export.csv';

function url(rel: string): string {
  return new URL(rel, document.baseURI).href;
}

function metricByKey(k: string): Metric {
  return METRICS.find((m) => m.key === k) ?? METRICS[0];
}

function setGroupVisibility(view: string) {
  document.querySelectorAll<HTMLElement>('.group[data-view]').forEach((el) => {
    el.hidden = el.dataset.view !== view;
  });
}

function populateMetricDropdown(sel: HTMLSelectElement, defaultKey?: string) {
  for (const m of METRICS) {
    const opt = document.createElement('option');
    opt.value = m.key;
    opt.textContent = m.label;
    sel.appendChild(opt);
  }
  if (defaultKey) sel.value = defaultKey;
}

async function main() {
  const db = await getDuckDB();
  const conn = await db.connect();

  populateMetricDropdown(metricSelect);
  populateMetricDropdown(xMetricSelect, 'income');
  populateMetricDropdown(yMetricSelect, 'home_value');

  const allOpt = document.createElement('option');
  allOpt.value = ALL_STATES_VALUE;
  allOpt.textContent = 'All states';
  stateSelect.appendChild(allOpt);

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
  stateSelect.value = '01';

  viewSelect.addEventListener('change', () => {
    setGroupVisibility(viewSelect.value);
    render(conn);
  });
  for (const sel of [metricSelect, xMetricSelect, yMetricSelect, stateSelect]) {
    sel.addEventListener('change', () => render(conn));
  }
  downloadBtn.addEventListener('click', downloadCsv);

  setGroupVisibility(viewSelect.value);
  metricSelect.disabled = false;
  stateSelect.disabled = false;
  downloadBtn.disabled = false;
  await render(conn);
}

async function render(conn: AsyncDuckDBConnection) {
  const stateFips = stateSelect.value;
  if (stateFips !== ALL_STATES_VALUE && !/^\d{2}$/.test(stateFips)) {
    throw new Error(`bad state_fips: ${stateFips}`);
  }
  statusEl.textContent = 'Querying…';

  if (viewSelect.value === 'scatter') {
    await renderScatter(conn, metricByKey(xMetricSelect.value), metricByKey(yMetricSelect.value), stateFips);
  } else if (viewSelect.value === 'histogram') {
    await renderHistogram(conn, stateFips);
  } else {
    await renderBar(conn, metricByKey(metricSelect.value), stateFips);
  }
}

const HISTOGRAM_PARQUET = 'data/parquet/acs_b25075_county_2020_2024.parquet';

async function renderHistogram(conn: AsyncDuckDBConnection, stateFips: string) {
  const isAll = stateFips === ALL_STATES_VALUE;
  const where = isAll ? '' : `WHERE state_fips = '${stateFips}'`;

  const result = await conn.query(`
    SELECT
      bracket_index,
      bracket_label,
      CAST(SUM(units) AS BIGINT) AS units,
      CAST(SUM(units) AS DOUBLE) / CAST(SUM(SUM(units)) OVER () AS DOUBLE) AS share
    FROM read_parquet('${url(HISTOGRAM_PARQUET)}')
    ${where}
    GROUP BY bracket_index, bracket_label
    ORDER BY bracket_index
  `);
  const rows = result.toArray() as { bracket_index: number; bracket_label: string; units: bigint; share: number }[];

  const totalUnits = rows.reduce((s, r) => s + Number(r.units), 0);
  const stateRow = await conn.query(
    isAll
      ? `SELECT 'United States' AS state_name`
      : `SELECT DISTINCT state_name FROM read_parquet('${url(HISTOGRAM_PARQUET)}') WHERE state_fips = '${stateFips}'`,
  );
  const stateName = (stateRow.toArray()[0] as { state_name: string }).state_name;

  currentRows = rows.map((r) => ({
    bracket_index: r.bracket_index,
    bracket_label: r.bracket_label,
    units: Number(r.units),
    share: r.share,
  }));
  currentCsvHeaders = ['bracket_index', 'bracket_label', 'units', 'share'];
  currentCsvFilename = `b25075_distribution_${slug(stateName)}.csv`;

  sourceEl.textContent = `Source: ACS table B25075, 5-year (${stateName}). ${totalUnits.toLocaleString()} owner-occupied housing units.`;

  chartEl.innerHTML = '';
  if (totalUnits === 0) {
    chartEl.textContent = 'No data.';
    statusEl.textContent = '';
    return;
  }

  const labels = rows.map((r) => r.bracket_label);
  const chart = Plot.plot({
    marginLeft: 70,
    marginBottom: 130,
    marginTop: 30,
    marginRight: 30,
    height: 520,
    x: { label: 'Home value bracket', domain: labels, tickRotate: -45 },
    y: { label: '↑ Share of owner-occupied units', grid: true, tickFormat: '.0%' },
    marks: [
      Plot.barY(rows, {
        x: 'bracket_label',
        y: 'share',
        fill: '#4c78a8',
      }),
      Plot.text(rows, {
        x: 'bracket_label',
        y: 'share',
        text: (d: { share: number }) => (d.share >= 0.005 ? `${(d.share * 100).toFixed(1)}%` : ''),
        textAnchor: 'middle',
        dy: -6,
        fontSize: 9,
        fill: '#333',
      }),
      Plot.ruleY([0]),
    ],
  });
  chartEl.appendChild(chart);

  statusEl.textContent = `${rows.length} brackets · ${totalUnits.toLocaleString()} units`;
}

async function renderBar(conn: AsyncDuckDBConnection, metric: Metric, stateFips: string) {
  const isAll = stateFips === ALL_STATES_VALUE;
  const where = isAll ? '' : `WHERE state_fips = '${stateFips}'`;
  const limit = isAll ? `LIMIT ${TOP_N_NATIONWIDE}` : '';

  const result = await conn.query(`
    SELECT * REPLACE (CAST(${metric.valueCol} AS DOUBLE) AS ${metric.valueCol})
    FROM read_parquet('${url(metric.parquet)}')
    ${where}
    ORDER BY ${metric.valueCol} DESC NULLS LAST
    ${limit}
  `);
  currentRows = result.toArray() as Row[];
  const stateName = isAll ? 'United States' : ((currentRows[0]?.state_name as string) ?? '');
  currentCsvHeaders = currentRows.length ? Object.keys(currentRows[0]) : [];
  currentCsvFilename = `${metric.table.toLowerCase()}_${slug(stateName)}_2020_2024.csv`;

  sourceEl.textContent = isAll
    ? `Source: ACS table ${metric.table}, 5-year (top ${TOP_N_NATIONWIDE} of all US counties).`
    : `Source: ACS table ${metric.table}, 5-year (${stateName}).`;

  chartEl.innerHTML = '';
  const plotted = currentRows.filter((r) => r[metric.valueCol] != null);
  if (plotted.length === 0) {
    chartEl.textContent = 'No data for this state.';
    statusEl.textContent = `${currentRows.length} counties (all null)`;
    return;
  }

  const labelFor = (d: Row) =>
    isAll ? `${d.county_name as string}, ${d.state_name as string}` : (d.county_name as string);

  const labeled = plotted.map((d) => ({ ...d, _label: labelFor(d) }));

  const chart = Plot.plot({
    marginLeft: isAll ? 240 : 200,
    marginRight: 90,
    height: Math.max(280, labeled.length * 20 + 60),
    x: { label: metric.label, grid: true, tickFormat: metric.axisFormat },
    y: { label: null },
    marks: [
      Plot.barX(labeled, {
        x: metric.valueCol,
        y: '_label',
        sort: { y: 'x', reverse: true },
        fill: '#4c78a8',
      }),
      Plot.text(labeled, {
        x: metric.valueCol,
        y: '_label',
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

async function renderScatter(conn: AsyncDuckDBConnection, x: Metric, y: Metric, stateFips: string) {
  if (x.key === y.key) {
    chartEl.innerHTML = '';
    chartEl.textContent = 'Pick two different metrics for X and Y.';
    statusEl.textContent = '';
    sourceEl.textContent = '';
    return;
  }

  const isAll = stateFips === ALL_STATES_VALUE;
  const where = isAll ? '' : `WHERE x.state_fips = '${stateFips}'`;

  const result = await conn.query(`
    SELECT
      x.geo_id, x.state_fips, x.state_name, x.county_name,
      CAST(x.${x.valueCol} AS DOUBLE) AS x_val,
      CAST(y.${y.valueCol} AS DOUBLE) AS y_val
    FROM read_parquet('${url(x.parquet)}') x
    JOIN read_parquet('${url(y.parquet)}') y USING (geo_id)
    ${where}
  `);
  const rows = result.toArray() as Row[];

  const valid = rows.filter((r) => r.x_val != null && r.y_val != null);

  let r2: number | null = null;
  if (valid.length >= 3) {
    const corrResult = await conn.query(`
      SELECT CORR(x_val, y_val) AS r FROM (
        SELECT
          CAST(x.${x.valueCol} AS DOUBLE) AS x_val,
          CAST(y.${y.valueCol} AS DOUBLE) AS y_val
        FROM read_parquet('${url(x.parquet)}') x
        JOIN read_parquet('${url(y.parquet)}') y USING (geo_id)
        ${where}
      ) WHERE x_val IS NOT NULL AND y_val IS NOT NULL
    `);
    const r = (corrResult.toArray()[0] as { r: number | null }).r;
    if (r != null) r2 = r * r;
  }

  currentRows = valid;
  currentCsvHeaders = ['geo_id', 'state_fips', 'state_name', 'county_name', 'x_val', 'y_val'];
  const stateSlug = isAll ? 'us' : slug((rows[0]?.state_name as string) ?? '');
  currentCsvFilename = `scatter_${x.table}_x_${y.table}_${stateSlug}.csv`;

  const stateName = isAll ? 'United States' : ((rows[0]?.state_name as string) ?? '');
  sourceEl.textContent =
    `Scatter: ${x.label} (${x.table}) vs. ${y.label} (${y.table}) — ${stateName}` +
    (r2 != null ? `. R² = ${r2.toFixed(3)}` : '');

  chartEl.innerHTML = '';
  if (valid.length === 0) {
    chartEl.textContent = 'No counties with both values.';
    statusEl.textContent = '0 counties';
    return;
  }

  const chart = Plot.plot({
    marginLeft: 80,
    marginBottom: 50,
    marginTop: 30,
    marginRight: 30,
    height: 600,
    grid: true,
    x: { label: `${x.label} →`, tickFormat: x.axisFormat },
    y: { label: `↑ ${y.label}`, tickFormat: y.axisFormat },
    marks: [
      Plot.dot(valid, {
        x: 'x_val',
        y: 'y_val',
        r: 3,
        fill: '#4c78a8',
        fillOpacity: 0.5,
        stroke: '#1f3b5a',
        strokeOpacity: 0.3,
        title: (d: Row) =>
          `${d.county_name}, ${d.state_name}\n${x.label}: ${x.rowFormat(d.x_val as number)}\n${y.label}: ${y.rowFormat(d.y_val as number)}`,
      }),
      Plot.linearRegressionY(valid, {
        x: 'x_val',
        y: 'y_val',
        stroke: '#c4453d',
        strokeOpacity: 0.6,
      }),
    ],
  });
  chartEl.appendChild(chart);

  const nullCount = rows.length - valid.length;
  statusEl.textContent = `${valid.length} counties${nullCount ? ` (${nullCount} dropped)` : ''}`;
}

function downloadCsv() {
  if (!currentRows.length) return;
  const lines = [currentCsvHeaders.join(',')];
  for (const r of currentRows) {
    lines.push(currentCsvHeaders.map((h) => csvCell(r[h])).join(','));
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
  const u = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = u;
  a.download = currentCsvFilename;
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

function slug(s: string): string {
  return s.replace(/\s+/g, '_').toLowerCase().replace(/[^a-z0-9_]/g, '');
}

main().catch((err) => {
  statusEl.textContent = `Error: ${err.message}`;
  console.error(err);
});

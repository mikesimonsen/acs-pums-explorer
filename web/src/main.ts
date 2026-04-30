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

type Distribution = {
  key: string;
  label: string;
  table: string;
  parquet: string;
  totalLabel: string;
  axisTitle: string;
  kind: 'histogram' | 'categorical';
};

const DISTRIBUTIONS: Distribution[] = [
  {
    key: 'home_value_brackets',
    label: 'Home value (B25075)',
    table: 'B25075',
    parquet: 'data/parquet/acs_b25075_county_2020_2024.parquet',
    totalLabel: 'owner-occupied housing units',
    axisTitle: 'Home value bracket',
    kind: 'histogram',
  },
  {
    key: 'income_brackets',
    label: 'Household income (B19001)',
    table: 'B19001',
    parquet: 'data/parquet/acs_b19001_county_2020_2024.parquet',
    totalLabel: 'households',
    axisTitle: 'Household income bracket',
    kind: 'histogram',
  },
  {
    key: 'year_built',
    label: 'Year structure built (B25034)',
    table: 'B25034',
    parquet: 'data/parquet/acs_b25034_county_2020_2024.parquet',
    totalLabel: 'housing units',
    axisTitle: 'Year built',
    kind: 'histogram',
  },
  {
    key: 'race_ethnicity',
    label: 'Race / ethnicity (B03002)',
    table: 'B03002',
    parquet: 'data/parquet/acs_b03002_county_2020_2024.parquet',
    totalLabel: 'people',
    axisTitle: 'Share',
    kind: 'categorical',
  },
  {
    key: 'education',
    label: 'Education attainment, 25+ (B15003)',
    table: 'B15003',
    parquet: 'data/parquet/acs_b15003_county_2020_2024.parquet',
    totalLabel: 'people age 25+',
    axisTitle: 'Share',
    kind: 'categorical',
  },
  {
    key: 'bedrooms',
    label: 'Bedrooms per unit (B25041)',
    table: 'B25041',
    parquet: 'data/parquet/acs_b25041_county_2020_2024.parquet',
    totalLabel: 'housing units',
    axisTitle: 'Bedrooms',
    kind: 'histogram',
  },
  {
    key: 'units_in_structure',
    label: 'Units in structure (B25024)',
    table: 'B25024',
    parquet: 'data/parquet/acs_b25024_county_2020_2024.parquet',
    totalLabel: 'housing units',
    axisTitle: 'Share',
    kind: 'categorical',
  },
  {
    key: 'employment',
    label: 'Employment status, 16+ (B23025)',
    table: 'B23025',
    parquet: 'data/parquet/acs_b23025_county_2020_2024.parquet',
    totalLabel: 'people age 16+',
    axisTitle: 'Share',
    kind: 'categorical',
  },
  {
    key: 'household_size',
    label: 'Household size (B11016)',
    table: 'B11016',
    parquet: 'data/parquet/acs_b11016_county_2020_2024.parquet',
    totalLabel: 'households',
    axisTitle: 'Household size',
    kind: 'histogram',
  },
];

const ALL_STATES_VALUE = 'all';
const ALL_COUNTIES_VALUE = 'all';
const TOP_N_NATIONWIDE = 50;

type PumsXtab = {
  key: string;
  label: string;
  needsPerson: boolean;
};

const PUMS_XTABS: PumsXtab[] = [
  {
    key: 'ownership_by_income_age',
    label: 'Homeownership rate × income decile × householder age',
    needsPerson: true,
  },
  {
    key: 'rent_burden_by_income',
    label: 'Rent burden distribution by income decile',
    needsPerson: false,
  },
];

// State FIPS codes that have PUMS Parquets in data/parquet/pums/.
const PUMS_HOUSING_STATES = ['06', '12', '36', '48', '56'];
const PUMS_PERSON_STATES = ['06', '12', '36', '56'];

const $ = <T extends HTMLElement>(id: string) => document.getElementById(id) as T;
const viewSelect = $<HTMLSelectElement>('view');
const metricSelect = $<HTMLSelectElement>('metric');
const xMetricSelect = $<HTMLSelectElement>('xMetric');
const yMetricSelect = $<HTMLSelectElement>('yMetric');
const distributionSelect = $<HTMLSelectElement>('distribution');
const countySelect = $<HTMLSelectElement>('county');
const pumsXtabSelect = $<HTMLSelectElement>('pumsXtab');
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

function distributionByKey(k: string): Distribution {
  return DISTRIBUTIONS.find((d) => d.key === k) ?? DISTRIBUTIONS[0];
}

function pumsXtabByKey(k: string): PumsXtab {
  return PUMS_XTABS.find((x) => x.key === k) ?? PUMS_XTABS[0];
}

function pumsHousingFiles(stateFipsList: string[]): string {
  return stateFipsList
    .map((s) => `'${url(`data/parquet/pums/h_${s}.parquet`)}'`)
    .join(', ');
}

function pumsPersonFiles(stateFipsList: string[]): string {
  return stateFipsList
    .map((s) => `'${url(`data/parquet/pums/p_${s}.parquet`)}'`)
    .join(', ');
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

  for (const d of DISTRIBUTIONS) {
    const opt = document.createElement('option');
    opt.value = d.key;
    opt.textContent = d.label;
    distributionSelect.appendChild(opt);
  }

  for (const x of PUMS_XTABS) {
    const opt = document.createElement('option');
    opt.value = x.key;
    opt.textContent = x.label;
    pumsXtabSelect.appendChild(opt);
  }

  const allCountyOpt = document.createElement('option');
  allCountyOpt.value = ALL_COUNTIES_VALUE;
  allCountyOpt.textContent = 'All counties (state aggregate)';
  countySelect.appendChild(allCountyOpt);

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
  stateSelect.addEventListener('change', () => {
    void refreshCountyDropdown(conn);
    render(conn);
  });
  for (const sel of [
    metricSelect,
    xMetricSelect,
    yMetricSelect,
    distributionSelect,
    countySelect,
    pumsXtabSelect,
  ]) {
    sel.addEventListener('change', () => render(conn));
  }
  downloadBtn.addEventListener('click', downloadCsv);

  setGroupVisibility(viewSelect.value);
  metricSelect.disabled = false;
  stateSelect.disabled = false;
  downloadBtn.disabled = false;
  await refreshCountyDropdown(conn);
  await render(conn);
}

async function refreshCountyDropdown(conn: AsyncDuckDBConnection) {
  while (countySelect.options.length > 1) countySelect.remove(1);
  const stateFips = stateSelect.value;
  if (stateFips === ALL_STATES_VALUE || !/^\d{2}$/.test(stateFips)) {
    countySelect.value = ALL_COUNTIES_VALUE;
    return;
  }
  const result = await conn.query(`
    SELECT county_fips, county_name
    FROM read_parquet('${url(METRICS[0].parquet)}')
    WHERE state_fips = '${stateFips}'
    ORDER BY county_name
  `);
  for (const row of result.toArray() as { county_fips: string; county_name: string }[]) {
    const opt = document.createElement('option');
    opt.value = row.county_fips;
    opt.textContent = row.county_name;
    countySelect.appendChild(opt);
  }
  countySelect.value = ALL_COUNTIES_VALUE;
}

async function render(conn: AsyncDuckDBConnection) {
  const stateFips = stateSelect.value;
  if (stateFips !== ALL_STATES_VALUE && !/^\d{2}$/.test(stateFips)) {
    throw new Error(`bad state_fips: ${stateFips}`);
  }
  statusEl.textContent = 'Querying…';

  if (viewSelect.value === 'scatter') {
    await renderScatter(conn, metricByKey(xMetricSelect.value), metricByKey(yMetricSelect.value), stateFips);
  } else if (viewSelect.value === 'distribution') {
    await renderDistribution(conn, distributionByKey(distributionSelect.value), stateFips, countySelect.value);
  } else if (viewSelect.value === 'pums') {
    await renderPums(conn, pumsXtabByKey(pumsXtabSelect.value), stateFips);
  } else {
    await renderBar(conn, metricByKey(metricSelect.value), stateFips);
  }
}

type DistRow = {
  bracket_index: number;
  bracket_label: string;
  units: number;
  share: number;
};

async function fetchDistribution(
  conn: AsyncDuckDBConnection,
  dist: Distribution,
  whereClause: string,
): Promise<DistRow[]> {
  const result = await conn.query(`
    SELECT
      bracket_index,
      bracket_label,
      CAST(SUM(units) AS BIGINT) AS units,
      CAST(SUM(units) AS DOUBLE) / NULLIF(CAST(SUM(SUM(units)) OVER () AS DOUBLE), 0) AS share
    FROM read_parquet('${url(dist.parquet)}')
    ${whereClause}
    GROUP BY bracket_index, bracket_label
    ORDER BY bracket_index
  `);
  return (result.toArray() as { bracket_index: number; bracket_label: string; units: bigint; share: number | null }[]).map(
    (r) => ({
      bracket_index: r.bracket_index,
      bracket_label: r.bracket_label,
      units: Number(r.units),
      share: r.share ?? 0,
    }),
  );
}

async function renderDistribution(
  conn: AsyncDuckDBConnection,
  dist: Distribution,
  stateFips: string,
  countyFips: string,
) {
  const isAllStates = stateFips === ALL_STATES_VALUE;
  const isCountySpecific = !isAllStates && countyFips !== ALL_COUNTIES_VALUE;

  const stateWhere = isAllStates ? '' : `WHERE state_fips = '${stateFips}'`;
  const stateRows = await fetchDistribution(conn, dist, stateWhere);
  const stateTotal = stateRows.reduce((s, r) => s + r.units, 0);

  let countyRows: DistRow[] | null = null;
  let countyName = '';
  if (isCountySpecific) {
    const countyWhere = `WHERE state_fips = '${stateFips}' AND county_fips = '${countyFips}'`;
    countyRows = await fetchDistribution(conn, dist, countyWhere);
    const opt = countySelect.options[countySelect.selectedIndex];
    countyName = opt ? opt.textContent ?? '' : '';
  }

  const stateName = isAllStates
    ? 'United States'
    : ((
        await conn.query(`
          SELECT DISTINCT state_name FROM read_parquet('${url(dist.parquet)}')
          WHERE state_fips = '${stateFips}'
        `)
      ).toArray()[0] as { state_name: string }).state_name;

  if (isCountySpecific && countyRows) {
    currentRows = [
      ...countyRows.map((r) => ({ ...r, scope: countyName })),
      ...stateRows.map((r) => ({ ...r, scope: `${stateName} (state)` })),
    ];
  } else {
    currentRows = stateRows;
  }
  currentCsvHeaders = isCountySpecific
    ? ['scope', 'bracket_index', 'bracket_label', 'units', 'share']
    : ['bracket_index', 'bracket_label', 'units', 'share'];
  currentCsvFilename = `${dist.table.toLowerCase()}_${slug(stateName)}${
    isCountySpecific ? `_${slug(countyName)}` : ''
  }.csv`;

  if (isCountySpecific) {
    const countyTotal = countyRows!.reduce((s, r) => s + r.units, 0);
    sourceEl.textContent = `${dist.label}: ${countyName} (${countyTotal.toLocaleString()} ${dist.totalLabel}) overlaid on ${stateName} aggregate (${stateTotal.toLocaleString()}).`;
  } else {
    sourceEl.textContent = `${dist.label}: ${stateName}. ${stateTotal.toLocaleString()} ${dist.totalLabel}.`;
  }

  chartEl.innerHTML = '';
  if (stateTotal === 0) {
    chartEl.textContent = 'No data.';
    statusEl.textContent = '';
    return;
  }

  if (dist.kind === 'histogram') {
    drawHistogram(dist, stateRows, isCountySpecific ? countyRows : null, countyName, stateName);
  } else {
    drawCategorical(dist, stateRows, isCountySpecific ? countyRows : null, countyName, stateName);
  }

  if (isCountySpecific) {
    statusEl.textContent = `${stateRows.length} brackets · ${countyName} vs ${stateName}`;
  } else {
    statusEl.textContent = `${stateRows.length} brackets · ${stateTotal.toLocaleString()} ${dist.totalLabel}`;
  }
}

function drawHistogram(
  dist: Distribution,
  stateRows: DistRow[],
  countyRows: DistRow[] | null,
  countyName: string,
  stateName: string,
) {
  const labels = stateRows.map((r) => r.bracket_label);
  const stateLabel = `${stateName} (state)`;

  const marks: Plot.Markish[] = [];
  if (countyRows) {
    marks.push(
      Plot.barY(countyRows, {
        x: 'bracket_label',
        y: 'share',
        fill: '#4c78a8',
        fillOpacity: 0.85,
      }),
      Plot.line(stateRows, {
        x: 'bracket_label',
        y: 'share',
        stroke: '#c4453d',
        strokeWidth: 2,
      }),
      Plot.dot(stateRows, {
        x: 'bracket_label',
        y: 'share',
        fill: '#c4453d',
        r: 3,
      }),
    );
  } else {
    marks.push(
      Plot.barY(stateRows, {
        x: 'bracket_label',
        y: 'share',
        fill: '#4c78a8',
      }),
      Plot.text(stateRows, {
        x: 'bracket_label',
        y: 'share',
        text: (d: DistRow) => (d.share >= 0.005 ? `${(d.share * 100).toFixed(1)}%` : ''),
        textAnchor: 'middle',
        dy: -6,
        fontSize: 9,
        fill: '#333',
      }),
    );
  }
  marks.push(Plot.ruleY([0]));

  const chart = Plot.plot({
    marginLeft: 70,
    marginBottom: 130,
    marginTop: 40,
    marginRight: 30,
    height: 520,
    x: { label: dist.axisTitle, domain: labels, tickRotate: -45 },
    y: { label: '↑ Share', grid: true, tickFormat: '.0%' },
    marks,
    color: countyRows
      ? {
          legend: true,
          domain: [countyName, stateLabel],
          range: ['#4c78a8', '#c4453d'],
        }
      : undefined,
  });
  chartEl.appendChild(chart);
}

function drawCategorical(
  _dist: Distribution,
  stateRows: DistRow[],
  countyRows: DistRow[] | null,
  countyName: string,
  stateName: string,
) {
  const labels = stateRows.map((r) => r.bracket_label);
  const stateLabel = `${stateName} (state)`;

  const marks: Plot.Markish[] = [];
  if (countyRows) {
    marks.push(
      Plot.barX(countyRows, {
        x: 'share',
        y: 'bracket_label',
        fill: '#4c78a8',
        fillOpacity: 0.85,
      }),
      Plot.tickX(stateRows, {
        x: 'share',
        y: 'bracket_label',
        stroke: '#c4453d',
        strokeWidth: 3,
      }),
      Plot.text(countyRows, {
        x: 'share',
        y: 'bracket_label',
        text: (d: DistRow) => (d.share >= 0.001 ? `${(d.share * 100).toFixed(1)}%` : ''),
        textAnchor: 'start',
        dx: 6,
        fontSize: 10,
        fill: '#1f3b5a',
      }),
    );
  } else {
    marks.push(
      Plot.barX(stateRows, {
        x: 'share',
        y: 'bracket_label',
        fill: '#4c78a8',
      }),
      Plot.text(stateRows, {
        x: 'share',
        y: 'bracket_label',
        text: (d: DistRow) => (d.share >= 0.001 ? `${(d.share * 100).toFixed(1)}%` : ''),
        textAnchor: 'start',
        dx: 4,
        fontSize: 10,
        fill: '#333',
      }),
    );
  }
  marks.push(Plot.ruleX([0]));

  const chart = Plot.plot({
    marginLeft: 320,
    marginRight: 80,
    marginTop: 40,
    height: Math.max(300, labels.length * 40 + 80),
    x: { label: '→ Share', grid: true, tickFormat: '.0%' },
    y: { label: null, domain: labels },
    marks,
    color: countyRows
      ? {
          legend: true,
          domain: [`${countyName} (bar)`, `${stateLabel} (tick)`],
          range: ['#4c78a8', '#c4453d'],
        }
      : undefined,
  });
  chartEl.appendChild(chart);
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

async function renderPums(conn: AsyncDuckDBConnection, xtab: PumsXtab, stateFips: string) {
  const isAll = stateFips === ALL_STATES_VALUE;
  const housingPool = isAll ? PUMS_HOUSING_STATES : PUMS_HOUSING_STATES.filter((s) => s === stateFips);
  const personPool = isAll ? PUMS_PERSON_STATES : PUMS_PERSON_STATES.filter((s) => s === stateFips);
  const pool = xtab.needsPerson ? personPool : housingPool;

  if (pool.length === 0) {
    chartEl.innerHTML = '';
    const need = xtab.needsPerson ? 'housing+person' : 'housing';
    const have = xtab.needsPerson ? PUMS_PERSON_STATES : PUMS_HOUSING_STATES;
    chartEl.textContent = `PUMS ${need} data is not available for the selected state. Available: ${have.join(', ')}.`;
    sourceEl.textContent = '';
    statusEl.textContent = '';
    return;
  }

  if (xtab.key === 'ownership_by_income_age') {
    await renderOwnershipHeatmap(conn, pool, isAll);
  } else if (xtab.key === 'rent_burden_by_income') {
    await renderRentBurdenStack(conn, pool, isAll);
  }
}

async function renderOwnershipHeatmap(conn: AsyncDuckDBConnection, states: string[], isAll: boolean) {
  const housing = pumsHousingFiles(states);
  const person = pumsPersonFiles(states);

  const sql = `
    WITH base AS (
      SELECT h.HINCP, h.WGTP, h.TEN, p.AGEP
      FROM read_parquet([${housing}]) h
      JOIN read_parquet([${person}]) p USING (SERIALNO)
      WHERE p.SPORDER = 1
        AND h.HINCP IS NOT NULL
        AND h.TEN BETWEEN 1 AND 4
    ),
    binned AS (
      SELECT
        WGTP, TEN,
        NTILE(10) OVER (ORDER BY HINCP) AS income_decile,
        CASE
          WHEN AGEP < 35 THEN '<35'
          WHEN AGEP < 50 THEN '35-49'
          WHEN AGEP < 65 THEN '50-64'
          ELSE '65+'
        END AS age_band
      FROM base
    )
    SELECT
      income_decile,
      age_band,
      CAST(SUM(WGTP) FILTER (WHERE TEN IN (1, 2)) AS DOUBLE) /
        NULLIF(CAST(SUM(WGTP) AS DOUBLE), 0) AS owner_rate,
      CAST(SUM(WGTP) AS BIGINT) AS households
    FROM binned
    GROUP BY income_decile, age_band
    ORDER BY income_decile, age_band
  `;
  const result = await conn.query(sql);
  const rows = (result.toArray() as { income_decile: bigint | number; age_band: string; owner_rate: number | null; households: bigint }[]).map(
    (r) => ({
      income_decile: Number(r.income_decile),
      age_band: r.age_band,
      owner_rate: r.owner_rate,
      households: Number(r.households),
    }),
  );

  const totalHouseholds = rows.reduce((s, r) => s + r.households, 0);
  const stateLabel = isAll ? `All PUMS states (${states.length})` : stateNameByFips(states[0]);

  currentRows = rows;
  currentCsvHeaders = ['income_decile', 'age_band', 'owner_rate', 'households'];
  currentCsvFilename = `pums_ownership_${slug(stateLabel)}.csv`;

  sourceEl.textContent = `PUMS 2024 1-year, weighted by WGTP. ${stateLabel}. ${totalHouseholds.toLocaleString()} households across ${rows.length} cells.`;

  chartEl.innerHTML = '';
  const chart = Plot.plot({
    width: 720,
    height: 420,
    marginLeft: 100,
    marginRight: 30,
    marginTop: 40,
    marginBottom: 60,
    padding: 0.04,
    x: {
      label: '→ Householder age band',
      domain: ['<35', '35-49', '50-64', '65+'],
      type: 'band',
    },
    y: {
      label: '↑ Income decile (10 = highest)',
      domain: [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
      type: 'band',
    },
    color: {
      type: 'linear',
      scheme: 'blues',
      label: 'Homeownership rate',
      tickFormat: '.0%',
      legend: true,
      domain: [0, 1],
    },
    marks: [
      Plot.cell(rows, { x: 'age_band', y: 'income_decile', fill: 'owner_rate', inset: 0.5 }),
      Plot.text(rows, {
        x: 'age_band',
        y: 'income_decile',
        text: (d: { owner_rate: number | null }) =>
          d.owner_rate == null ? '—' : `${(d.owner_rate * 100).toFixed(0)}%`,
        fill: (d: { owner_rate: number | null }) => ((d.owner_rate ?? 0) > 0.55 ? 'white' : '#222'),
        fontSize: 11,
      }),
    ],
  });
  chartEl.appendChild(chart);

  statusEl.textContent = `${rows.length} cells · ${totalHouseholds.toLocaleString()} weighted households`;
}

async function renderRentBurdenStack(conn: AsyncDuckDBConnection, states: string[], isAll: boolean) {
  const housing = pumsHousingFiles(states);
  const burdenOrder = [
    'Not burdened (<30%)',
    'Cost-burdened (30-49%)',
    'Severely burdened (50%+)',
  ];

  const sql = `
    WITH renters AS (
      SELECT HINCP, WGTP, GRPIP
      FROM read_parquet([${housing}])
      WHERE TEN = 3 AND HINCP IS NOT NULL AND GRPIP IS NOT NULL
    ),
    binned AS (
      SELECT
        WGTP,
        NTILE(10) OVER (ORDER BY HINCP) AS income_decile,
        CASE
          WHEN GRPIP >= 50 THEN 'Severely burdened (50%+)'
          WHEN GRPIP >= 30 THEN 'Cost-burdened (30-49%)'
          ELSE 'Not burdened (<30%)'
        END AS burden
      FROM renters
    )
    SELECT
      income_decile,
      burden,
      CAST(SUM(WGTP) AS DOUBLE) /
        NULLIF(CAST(SUM(SUM(WGTP)) OVER (PARTITION BY income_decile) AS DOUBLE), 0) AS share,
      CAST(SUM(WGTP) AS BIGINT) AS households
    FROM binned
    GROUP BY income_decile, burden
    ORDER BY income_decile,
      CASE burden
        WHEN 'Not burdened (<30%)' THEN 1
        WHEN 'Cost-burdened (30-49%)' THEN 2
        WHEN 'Severely burdened (50%+)' THEN 3
      END
  `;
  const result = await conn.query(sql);
  const rows = (result.toArray() as { income_decile: bigint | number; burden: string; share: number | null; households: bigint }[]).map(
    (r) => ({
      income_decile: Number(r.income_decile),
      burden: r.burden,
      share: r.share ?? 0,
      households: Number(r.households),
    }),
  );

  const totalRenters = rows.reduce((s, r) => s + r.households, 0);
  const stateLabel = isAll ? `All PUMS states (${states.length})` : stateNameByFips(states[0]);

  currentRows = rows;
  currentCsvHeaders = ['income_decile', 'burden', 'share', 'households'];
  currentCsvFilename = `pums_rent_burden_${slug(stateLabel)}.csv`;

  sourceEl.textContent = `PUMS 2024 1-year, weighted by WGTP. Renter households (TEN=3) in ${stateLabel}. ${totalRenters.toLocaleString()} weighted renters.`;

  chartEl.innerHTML = '';
  const chart = Plot.plot({
    width: 720,
    height: 440,
    marginLeft: 60,
    marginRight: 30,
    marginTop: 40,
    marginBottom: 60,
    x: {
      label: 'Income decile (10 = highest)',
      domain: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      type: 'band',
    },
    y: { label: '↑ Share of renter households', tickFormat: '.0%', grid: true, domain: [0, 1] },
    color: {
      legend: true,
      domain: burdenOrder,
      range: ['#a6cee3', '#fdae61', '#d73027'],
    },
    marks: [
      Plot.barY(rows, {
        x: 'income_decile',
        y: 'share',
        fill: 'burden',
        order: burdenOrder,
      }),
      Plot.ruleY([0]),
    ],
  });
  chartEl.appendChild(chart);

  statusEl.textContent = `${rows.length} rows · ${totalRenters.toLocaleString()} weighted renters`;
}

const STATE_FIPS_TO_NAME: Record<string, string> = {
  '06': 'California',
  '12': 'Florida',
  '36': 'New York',
  '48': 'Texas',
  '56': 'Wyoming',
};

function stateNameByFips(fips: string): string {
  return STATE_FIPS_TO_NAME[fips] ?? fips;
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

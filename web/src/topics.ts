// Topic-first catalog: maps the 10 high-level topics from DATA_CATALOG.md
// to the existing analysis registry in main.ts (Metric / Distribution / PumsXtab keys).
// Stub entries describe analyses we want but haven't ingested data for yet —
// they show as disabled cards on the topic page and double as a roadmap.

export type AnalysisRef =
  | { kind: 'bar'; metric: string; title: string; hook: string }
  | { kind: 'distribution'; distribution: string; title: string; hook: string }
  | { kind: 'pums'; xtab: string; title: string; hook: string }
  | { kind: 'scatter'; x: string; y: string; title: string; hook: string }
  | { kind: 'stub'; title: string; hook: string; needs: string };

export type Topic = {
  id: string;
  name: string;
  blurb: string;
  analyses: AnalysisRef[];
};

export const TOPICS: Topic[] = [
  {
    id: 'people',
    name: 'People',
    blurb: 'Population, age, sex, race & ethnicity, immigration, language, veterans.',
    analyses: [
      { kind: 'bar', metric: 'population', title: 'Total population', hook: 'Where Americans actually live, ranked by county.' },
      { kind: 'bar', metric: 'median_age', title: 'Median age', hook: 'Retirement havens vs. college towns vs. immigrant-driven young counties.' },
      { kind: 'distribution', distribution: 'race_ethnicity', title: 'Race & ethnicity (county)', hook: 'Composition of any county broken into seven categories.' },
      { kind: 'pums', xtab: 'race_by_metro', title: 'Race & ethnicity by metro', hook: 'Same composition view but for any metro area, weighted from PUMS.' },
      { kind: 'pums', xtab: 'age_by_metro', title: 'Age distribution by metro', hook: 'Population pyramid silhouette without the sex split.' },
      { kind: 'pums', xtab: 'sex_age_pyramid', title: 'Sex × age pyramid', hook: 'The classic two-sided demographic chart, by metro.' },
      { kind: 'pums', xtab: 'citizenship_by_metro', title: 'Citizenship status', hook: 'Native, naturalized, non-citizen breakdown for any metro.' },
      { kind: 'pums', xtab: 'language_by_metro', title: 'Language spoken at home', hook: 'Where America speaks Spanish, Asian, or Indo-European languages.' },
      { kind: 'stub', title: 'Foreign-born share by county', hook: 'Immigrant settlement is concentrated — the map is more uneven than people guess.', needs: 'ACS B05002 ingestion' },
      { kind: 'stub', title: 'Veteran density by county', hook: 'Concentrations near bases and in retiree-heavy regions.', needs: 'ACS B21001 ingestion' },
    ],
  },
  {
    id: 'households',
    name: 'Households & Families',
    blurb: 'Composition, multigenerational living, single parents, marriage, fertility.',
    analyses: [
      { kind: 'distribution', distribution: 'household_size', title: 'Household size', hook: 'From 1.7 in college-town counties to 4+ in immigrant counties.' },
      { kind: 'stub', title: 'Multigenerational households', hook: 'Three+ generations under one roof — rising since 2008.', needs: 'PUMS MULTG analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'Single-parent households', hook: 'County-level map; strong correlate with poverty and school outcomes.', needs: 'ACS B09005 ingestion' },
      { kind: 'stub', title: 'Living-alone share', hook: 'Rising secular trend; key predictor of studio/1BR demand.', needs: 'ACS B11001 ingestion' },
      { kind: 'stub', title: 'Births in past year', hook: 'Where America is having children, by metro × age × marital status.', needs: 'PUMS FER analysis (data ingested, viz needed)' },
    ],
  },
  {
    id: 'housing',
    name: 'Housing',
    blurb: 'Tenure, value, rent, costs, vintage, structure, vacancy, recent movers.',
    analyses: [
      { kind: 'bar', metric: 'home_value', title: 'Median home value', hook: 'County rankings — the headline housing metric.' },
      { kind: 'bar', metric: 'rent', title: 'Median gross rent', hook: 'Where renters pay the most each month.' },
      { kind: 'bar', metric: 'ownership', title: 'Homeownership rate', hook: 'Coastal renter cities vs. heartland owner-majority counties.' },
      { kind: 'bar', metric: 'vacancy_rate', title: 'Vacancy rate', hook: 'Seasonal-vacation, for-rent, and abandonment patterns mixed together.' },
      { kind: 'bar', metric: 'housing_units', title: 'Total housing units', hook: 'How big the market actually is, county by county.' },
      { kind: 'distribution', distribution: 'home_value_brackets', title: 'Home value distribution', hook: 'Share of homes worth >$1M, by county — the affordability shift.' },
      { kind: 'distribution', distribution: 'year_built', title: 'Year structure built', hook: 'Pre-1940 vs. post-2010 — separates rust-belt from sun-belt at a glance.' },
      { kind: 'distribution', distribution: 'units_in_structure', title: 'Units in structure', hook: 'Single-family vs. multi-family share — direct read on land-use.' },
      { kind: 'distribution', distribution: 'bedrooms', title: 'Bedrooms per unit', hook: 'Studio-heavy vs. family-sized housing stock.' },
      { kind: 'pums', xtab: 'home_value_by_metro', title: 'Home value distribution by metro', hook: 'Top 6 metros faceted side-by-side from PUMS records.' },
      { kind: 'pums', xtab: 'ownership_by_income_age', title: 'Homeownership × income × age', hook: 'The cross-tab ACS doesn\'t publish — most-requested housing chart.' },
      { kind: 'pums', xtab: 'rent_burden_by_income', title: 'Rent burden by income', hook: 'Distribution of cost-burdened renters across income deciles.' },
      { kind: 'pums', xtab: 'mortgage_burden_by_income', title: 'Mortgage burden by income', hook: 'Same view for owners with mortgages.' },
      { kind: 'pums', xtab: 'recent_movers_by_tenure_metro', title: 'Recent movers by tenure × metro', hook: 'Who moved in the past year, owners vs. renters.' },
      { kind: 'scatter', x: 'income', y: 'home_value', title: 'Income vs. home value', hook: 'County-level affordability — how far home prices have decoupled from incomes.' },
      { kind: 'stub', title: 'Cost burden by county (ACS)', hook: 'County-granularity rent and mortgage cost burden.', needs: 'ACS B25070, B25091, B25093 ingestion' },
    ],
  },
  {
    id: 'money',
    name: 'Money',
    blurb: 'Household income, earnings, poverty, inequality, public assistance.',
    analyses: [
      { kind: 'bar', metric: 'income', title: 'Median household income', hook: 'County rankings; the most-cited single income number.' },
      { kind: 'distribution', distribution: 'income_brackets', title: 'Household income distribution', hook: 'Full bracket breakdown — bimodal in some counties.' },
      { kind: 'pums', xtab: 'income_by_metro', title: 'Income distribution by metro', hook: 'Where high earners cluster, weighted from PUMS.' },
      { kind: 'stub', title: 'Poverty rate by age', hook: 'Senior poverty vs. child poverty maps look completely different.', needs: 'ACS B17001 ingestion' },
      { kind: 'stub', title: 'Gini index', hook: 'County-level inequality — surprising peaks in resort and college towns.', needs: 'ACS B19083 ingestion' },
      { kind: 'stub', title: 'SNAP participation rate', hook: 'Share on food stamps; a clean read on economic stress.', needs: 'ACS B22001 ingestion' },
    ],
  },
  {
    id: 'work',
    name: 'Work',
    blurb: 'Employment, occupation, industry, hours, self-employment.',
    analyses: [
      { kind: 'distribution', distribution: 'employment', title: 'Employment status', hook: 'Labor-force participation and unemployment, county by county.' },
      { kind: 'stub', title: 'Industry concentration', hook: '% of workers in healthcare, manufacturing, finance — economic-base map.', needs: 'PUMS INDP analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'Occupation by metro', hook: 'Knowledge-worker share, blue-collar share, by metro.', needs: 'PUMS OCCP analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'Self-employed share', hook: 'Leading indicator of gig-economy and creative-class concentration.', needs: 'PUMS COW analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'Female labor-force participation', hook: 'Wide range across metros; correlates with childcare costs and college attainment.', needs: 'PUMS ESR × SEX analysis (data ingested, viz needed)' },
    ],
  },
  {
    id: 'commute',
    name: 'Getting Around',
    blurb: 'Commute mode, time, vehicles, work-from-home.',
    analyses: [
      { kind: 'stub', title: 'Commute mode', hook: 'Where Americans walk, bike, or take transit — concentrated in a handful of metros.', needs: 'PUMS JWTRNS analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'Commute time distribution', hook: 'Super-commuters: share with >90-minute commutes, growing in expensive metros\' exurbs.', needs: 'PUMS JWMNP analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'Work from home', hook: 'Post-2020 share by metro × industry × income — the killer cut.', needs: 'PUMS JWTRNS=11 analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'Vehicles per household', hook: 'The "no car" map — strong predictor of urbanism.', needs: 'ACS B25044 ingestion' },
    ],
  },
  {
    id: 'education',
    name: 'Education',
    blurb: 'Attainment, enrollment, fields of study.',
    analyses: [
      { kind: 'distribution', distribution: 'education', title: 'Educational attainment, 25+', hook: 'High-school-or-less vs. bachelor\'s+ — defining map of modern America.' },
      { kind: 'pums', xtab: 'education_by_metro', title: 'Education attainment by metro', hook: 'Same view from PUMS, sliceable to any metro.' },
      { kind: 'stub', title: 'Field of degree', hook: 'Where engineers cluster vs. humanities majors — STEM dominance maps.', needs: 'PUMS FOD1P analysis (data ingested, viz needed)' },
    ],
  },
  {
    id: 'health',
    name: 'Health & Coverage',
    blurb: 'Insurance type, uninsured rate, disability.',
    analyses: [
      { kind: 'pums', xtab: 'disability_by_age', title: 'Disability rate by age', hook: 'How disability prevalence climbs by age band, for any metro.' },
      { kind: 'stub', title: 'Uninsured rate by metro', hook: 'The single most-republished health-coverage chart in U.S. media.', needs: 'PUMS HICOV analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'Coverage type by age', hook: 'Medicare jumps at 65; private vs. public split below it.', needs: 'PUMS HINS1-7 analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'Disability by type', hook: 'Hearing, vision, cognitive, ambulatory — different age profiles, different geographies.', needs: 'PUMS DEAR/DEYE/DREM/DPHY analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'Uninsured rate by county', hook: 'Same story at county granularity; the Medicaid-expansion gap visible across state lines.', needs: 'ACS B27001 ingestion' },
    ],
  },
  {
    id: 'connectivity',
    name: 'Connectivity',
    blurb: 'Computers, internet, broadband.',
    analyses: [
      { kind: 'stub', title: 'Broadband subscription rate', hook: 'Tract-level digital divide; rural Black Belt and tribal lands stand out.', needs: 'ACS B28002 ingestion' },
      { kind: 'stub', title: 'No-computer households', hook: 'Senior digital exclusion, school-age children without home computers.', needs: 'ACS B28001 ingestion' },
      { kind: 'stub', title: 'Cellular-only households', hook: 'Households whose only home internet is a phone plan.', needs: 'ACS B28011 ingestion' },
    ],
  },
  {
    id: 'migration',
    name: 'Where People Move',
    blurb: 'Recent movers, in/out flows, length of residence.',
    analyses: [
      { kind: 'pums', xtab: 'recent_movers_by_tenure_metro', title: 'Recent movers by tenure × metro', hook: 'Who moved in the past year, owners vs. renters.' },
      { kind: 'stub', title: 'Where Californians went', hook: 'State-to-state migration flows from PUMS MIGSP.', needs: 'PUMS MIGSP analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'In-migration by metro', hook: 'Net flow of new residents over the past year.', needs: 'PUMS MIGSP × current state analysis (data ingested, viz needed)' },
      { kind: 'stub', title: 'Length of residence', hook: 'Housing-market churn: Detroit looks nothing like Phoenix on this metric.', needs: 'PUMS MV / ACS B25038 analysis (data ingested, viz needed)' },
    ],
  },
];

export function topicById(id: string): Topic | undefined {
  return TOPICS.find((t) => t.id === id);
}

export function totalAvailable(t: Topic): number {
  return t.analyses.filter((a) => a.kind !== 'stub').length;
}

export function totalStub(t: Topic): number {
  return t.analyses.filter((a) => a.kind === 'stub').length;
}

// Encode an analysis ref into a hash-route fragment.
export function analysisHash(a: AnalysisRef): string {
  switch (a.kind) {
    case 'bar': return `#/a/bar/${a.metric}`;
    case 'distribution': return `#/a/distribution/${a.distribution}`;
    case 'pums': return `#/a/pums/${a.xtab}`;
    case 'scatter': return `#/a/scatter/${a.x}/${a.y}`;
    case 'stub': return '';
  }
}

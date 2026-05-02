# ACS + PUMS Data Catalog

A topical map of what the American Community Survey (ACS) and Public Use Microdata Sample (PUMS) actually measure, the kinds of stories people tell with each topic, and what we've already ingested vs. what's still on the shelf. Intended as the implementation guide for a topic-first browse UI ("What can I learn about America?") instead of a chart-first one.

---

## Two data products, one survey

The ACS surveys ~3.5 million U.S. households per year. The Census Bureau publishes it two ways:

- **ACS published tables** — pre-aggregated counts and medians, available at every geography from nation down to census tract and block group. ~1,400 detailed tables (B-tables) and ~50 data profiles (DP, S, CP tables). You query by table ID; the Bureau has already done the math. **Strength:** small geographies, margins of error included. **Weakness:** only the cross-tabs the Bureau chose to publish.
- **PUMS microdata** — anonymized individual household and person records, ~1% of households per year (1-year file) or ~5% over 5 years. You can compute *any* cross-tab you want. **Strength:** unlimited cross-tabs, weighted to population. **Weakness:** smallest geography is a PUMA (~100,000 people), and you have to weight every estimate yourself.

Rule of thumb: if the question is "what's the value of X in tract/county/CBSA Y", use ACS tables. If the question is "for households like Z, how does X relate to Y", use PUMS.

### Geographic levels available

| Level | ACS tables | PUMS |
|---|---|---|
| Nation | ✓ | ✓ |
| State | ✓ | ✓ |
| Metro (CBSA) | ✓ | via PUMA→CBSA crosswalk |
| County | ✓ | partial (large counties) |
| County subdivision | ✓ | – |
| Place (city) | ✓ | – |
| PUMA (~100k pop) | ✓ | ✓ (native unit) |
| Census tract (~4k pop) | ✓ (5-year only) | – |
| Block group (~1.5k pop) | ✓ (limited subset) | – |

### What we've ingested so far

Already in `data/parquet/`:

- **17 ACS county tables, 5-year 2020–2024:** B01002 (median age), B01003 (population), B03002 (race × Hispanic origin), B11016 (household type & size), B15003 (educational attainment), B19001 (income distribution), B19013 (median household income), B23025 (employment status), B25001 (housing units), B25002 (vacancy), B25003 (tenure), B25024 (units in structure), B25034 (year built), B25041 (bedrooms), B25064 (median gross rent), B25075 (home value distribution), B25077 (median home value).
- **PUMS 2024 1-year, 5 states (CA, FL, NY, TX, WY):** housing records (tenure, value, rent, cost burden, household income, move-in, bedrooms, rooms, year built, household language) and person records (age, race, Hispanic origin, education, sex, citizenship, disability).
- **PUMA→CBSA crosswalk** so PUMS can be aggregated to metros.

The catalog below marks each topic ✅ ingested, 🟡 partially ingested, ⚪ not yet ingested.

---

## 1. Population & demographics ✅

The basics: how many people, where, how old, of what background.

**Key ACS tables:** B01001 (age × sex), B01002 (median age), B01003 (total population), B02001 (race), B03002 (Hispanic origin × race), B05001/B05002 (citizenship & place of birth), B16001 (language spoken at home), B21001 (veteran status).

**PUMS variables:** AGEP, SEX, RAC1P, HISP, CIT, NATIVITY, LANX, ENG, MIL.

**Compelling stories people tell:**
- **The aging map.** Median age by county reveals retirement-magnet counties (Florida, Arizona, the Ozarks, parts of Maine) vs. college-town outliers and immigrant-driven young counties.
- **The diversity index.** A single number per county — the probability two random people are of different race/ethnicity — produces a striking national map and is widely cited.
- **Where America speaks Spanish (or Vietnamese, or Tagalog).** Largest non-English language by county/tract. Punchy choropleth that always goes viral.
- **Foreign-born share, mapped.** Immigrant settlement is highly concentrated; the map is more uneven than people guess.
- **Veteran density.** Where vets live is a function of base proximity, retirement patterns, and history — surprising peaks in places like Hampton Roads, San Diego, Fayetteville NC.

**Our coverage:** B01002, B01003, B03002 ingested. Foreign-born / language / veterans not yet.

---

## 2. Households & families ✅

Who lives with whom. The unit of analysis for housing economics.

**Key ACS tables:** B11001 (household type), B11003 (family type), B11005 (households with children), B11016 (household type by size), B09005 (children under 18 by family type), B10063 (grandparents responsible for grandchildren), B11009 (unmarried-partner households), B12001 (marital status).

**PUMS variables:** HHT, NPF, NP, MAR, MARHT, MARHYP, MULTG, FES, PARTNER.

**Compelling stories:**
- **Multigenerational households.** Share of households with three+ generations under one roof — rising since 2008, with strong geographic and ethnic patterns. A great housing-affordability proxy.
- **Single-parent households by county.** Strongly correlates with poverty, school outcomes; a politically sensitive map that's worth showing carefully.
- **Average household size.** From 1.7 in college-town counties to 4+ in immigrant Latino counties. Direct input to housing demand.
- **Grandparents raising grandchildren.** A small but emotionally powerful indicator; high in Appalachia and parts of the rural South.
- **Living-alone share.** Rising secular trend; key predictor of studio/1BR demand and senior services.

**Our coverage:** B11016 ingested. Family-type and multigenerational tables not yet.

---

## 3. Housing — tenure, costs, structure ✅

The Compass core. Already the most-built-out part of the project.

**Key ACS tables:** B25001 (units), B25002 (occupancy/vacancy), B25003 (tenure), B25004 (vacancy reason), B25024 (units in structure), B25034 (year built), B25041 (bedrooms), B25064 (median gross rent), B25070 (rent as % of income), B25077 (median home value), B25075 (home value distribution), B25091 (mortgage cost burden), B25093 (homeowner cost burden), DP04 (housing profile rollup).

**PUMS variables:** TEN, VALP, RNTP, GRPIP, OCPIP, HINCP, MV, MVYR, BDSP, RMSP, YRBLT, BLD, VACS.

**Compelling stories:**
- **The cost-burden crisis.** % of renters paying >30% (or >50%) of income on rent, by metro and by income quintile. PUMS lets you split renters vs owners, by age, by race — ACS B25070/B25091 give county-level burden but no further cross-tabs.
- **Vintage of the housing stock.** % built before 1940, % built since 2010 — separates the rust-belt from the sun-belt at a glance.
- **The vacancy map.** Distinguishing seasonal (Cape Cod, ski towns) from for-rent (oversupplied) from "other vacant" (abandonment).
- **Single-family vs. multi-family share by metro.** Direct read on land-use policy and affordability outcomes.
- **Where 50% renter is normal.** Tenure flips by neighborhood; mapping the renter-majority tracts is a strong New York Times–style piece.
- **Million-dollar zip code spread.** Share of homes valued >$1M by tract — has expanded dramatically; tells the affordability story.

**Our coverage:** B25001, B25002, B25003, B25024, B25034, B25041, B25064, B25075, B25077 all ingested. Cost-burden tables (B25070, B25091, B25093) not yet — these are the missing piece for the burden story at county granularity.

---

## 4. Income, earnings & poverty ✅

**Key ACS tables:** B19001 (household income brackets), B19013 (median household income), B19025 (aggregate income), B19083 (Gini index), B19301 (per capita income), B17001 (poverty by age × sex), B17017 (poverty by household type), B19057 (public assistance income), B22001 (SNAP/food stamps), B19059 (retirement income), B20002 (median earnings by sex).

**PUMS variables:** HINCP, FINCP, PINCP, WAGP, SEMP, INTP, RETP, SSP, SSIP, PAP, OIP, POVPIP, ESR.

**Compelling stories:**
- **The Gini map.** County-level income inequality; some of the highest scores are in places people don't expect (resort towns, college towns) for very different reasons than urban cores.
- **Poverty by age.** Senior poverty vs. child poverty maps look completely different — a story most coverage misses.
- **SNAP participation rate.** Share of households on food stamps; a clean read on economic stress that updates yearly.
- **Earnings gap by sex.** Median female / median male earnings by county.
- **The income decile map.** Once you have PUMS, you can plot e.g. share of population in the top national decile by metro — Bay Area / NYC dominance becomes vivid.

**Our coverage:** B19001, B19013 ingested. Poverty (B17xxx), Gini (B19083), SNAP (B22001), per-capita (B19301) not yet.

---

## 5. Employment, occupation & industry 🟡

**Key ACS tables:** B23025 (employment status), B23001 (employment by age × sex), C24010 (occupation by sex), C24030 (industry by sex), B24011 (median earnings by occupation), B24080 (class of worker), B23022 (work hours).

**PUMS variables:** ESR, INDP, OCCP, COW, WKHP, WKWN, JWMNP (commute time), JWTRNS (commute mode), POWPUMA (place of work).

**Compelling stories:**
- **The remote-work map.** Share working from home, by metro, post-2020. PUMS lets you slice by industry, income, education — that's the killer cut.
- **Industry concentration.** "% of workers in healthcare/manufacturing/finance/agriculture" by county — reads like an economic-base map.
- **Self-employed share.** A leading indicator of gig-economy / creative-class concentration.
- **Female labor-force participation by metro.** Wide range; correlates with childcare costs, marriage age, college-attainment.
- **Average commute time.** From 15 min in flyover metros to 45+ in NYC/DC/SF. Direct input to housing-location stories.

**Our coverage:** B23025 (employment status only) ingested. Occupation, industry, commute, work-from-home not yet.

---

## 6. Commuting & transportation ⚪

**Key ACS tables:** B08006 (means of transport to work), B08013 (aggregate travel time), B08303 (travel time distribution), B08301 (means of transport for workers), B08137 (means by tenure), B25044 (vehicles available by tenure).

**PUMS variables:** JWMNP, JWTRNS, JWAP, JWDP (departure/arrival time), VEH.

**Compelling stories:**
- **Where Americans walk, bike, or take transit.** A "transit map" of the U.S. is brutally concentrated — NYC, DC, Boston, SF, Chicago, plus a handful of college towns. Surprising visualization.
- **The "no car" map.** % of households with zero vehicles. Strong predictor of urbanism and a key housing-design input.
- **Super-commuters.** Share of workers with >90-minute commutes — has grown sharply, concentrated in exurban counties of expensive metros.
- **Departure-time distribution.** When America leaves for work — bimodal vs. unimodal patterns by industry.

**Our coverage:** None ingested. Big gap, big story potential.

---

## 7. Education ✅

**Key ACS tables:** B15003 (educational attainment, age 25+), B14001 (school enrollment), B14007 (enrollment by level), B15010 (field of bachelor's degree), S1501 (educational attainment summary).

**PUMS variables:** SCHL, SCH, SCHG, FOD1P (field of degree).

**Compelling stories:**
- **% with a bachelor's degree, by county.** A defining map of modern America — splits the country into educational halves more cleanly than almost any other variable.
- **Field of degree concentration.** Where engineers cluster, where humanities majors cluster — STEM dominance maps.
- **The high-school-or-less share.** The other side of the BA map; key economic/political variable.
- **Enrollment among 18–24-year-olds.** Where college-going is normative vs. the exception.

**Our coverage:** B15003 ingested. Field of degree (B15010, FOD1P) not yet — that's the most novel data here.

---

## 8. Health insurance coverage ⚪

The "% uninsured" map you saw — this is the topic.

**Key ACS tables:** B27001 (coverage by age × sex), B27010 (types of coverage), B27011 (coverage by employment status), C27006 (Medicare), C27007 (Medicaid), C27008 (TRICARE/military), B992701 (coverage rate).

**PUMS variables:** HICOV, PRIVCOV, PUBCOV, HINS1–7 (each coverage type as a flag).

**Compelling stories:**
- **The uninsured map.** Single most-republished health-coverage chart in U.S. media. Texas + Florida + Mississippi vs. expanded-Medicaid states show up vividly.
- **Coverage type by age.** Medicare jumps at 65; the under-19 vs. 19–64 vs. 65+ stratification is essential.
- **The Medicaid expansion gap.** Adjacent counties across state lines (e.g., TN vs. KY, MO vs. IL) show step changes in uninsured rates.
- **Employer-sponsored coverage decline.** Trend cut over recent years.
- **Public vs. private coverage by income.** Cleanly available from PUMS.

**Our coverage:** None. High-impact addition; ACS publishes B27xxx at county level.

---

## 9. Disability ⚪

**Key ACS tables:** B18101 (disability by age × sex), B18102 (hearing), B18103 (vision), B18104 (cognitive), B18105 (ambulatory), B18106 (self-care), B18107 (independent living), S1810 (disability summary).

**PUMS variables:** DIS, DEAR, DEYE, DREM, DPHY, DDRS, DOUT.

**Compelling stories:**
- **Disability-rate map.** Wide variation, peaks in Appalachia, the Mississippi Delta, parts of Maine. Strong story about regional health.
- **Cognitive vs. ambulatory disability.** Different age profiles, different geographies.
- **Working-age disability.** % of 18–64-year-olds with a disability — labor-force and SSDI implications.

**Our coverage:** DIS ingested in PUMS. ACS B18xxx tables not yet.

---

## 10. Migration & geographic mobility 🟡

**Key ACS tables:** B07001 (geographic mobility by age), B07003 (mobility by sex), B07009 (mobility by educational attainment), B07010 (mobility by income), B07401 (movers from abroad), B25038 (year householder moved in).

**PUMS variables:** MIGSP, MIGPUMA, MV, MVYR.

**Compelling stories:**
- **Recent-mover share.** Share of people who moved in the past year — varies dramatically; high in college towns, military towns, and growing Sun Belt metros.
- **Where Californians went.** Out-migration flows by destination state. PUMS MIGSP × current state gives state-to-state migration; ACS tables aggregate to county-of-origin.
- **Length-of-residence in current home.** B25038 / MV — proxy for housing-market churn. Very different in Detroit vs. Phoenix.
- **In-migration vs. out-migration balance, by metro.** Net flows reveal which cities are growing organically vs. through births alone.

**Our coverage:** PUMS MV ingested for housing units. Migration tables (B07xxx) and MIGSP not yet — these are the engine for any "where people are moving" story.

---

## 11. Computer & internet access ⚪

**Key ACS tables:** B28001 (computers in household), B28002 (internet subscription), B28003 (computer × internet), B28011 (subscription type — broadband/dial-up/cellular only).

**PUMS variables:** ACCESS, LAPTOP, BROADBND, COMPOTHX.

**Compelling stories:**
- **The broadband gap, mapped.** Tract-level broadband subscription rates make the digital divide visceral; rural Black Belt and tribal lands stand out.
- **Cellular-only households.** People whose only home internet is a phone plan — a measure of fragile connectivity.
- **No-computer households by age.** Senior digital exclusion, school-age children without home computers.

**Our coverage:** None. A small set of tables but produces compelling, recent-vintage stories.

---

## 12. Marriage, fertility & living arrangements ⚪

**Key ACS tables:** B12001 (marital status), B12002 (marital status by age), B12503 (marriages in past year), B12504 (divorces in past year), B13002 (women with births in past 12 months), B11009 (unmarried-partner households).

**PUMS variables:** MAR, MARHT, MARHYP, FER, PARTNER.

**Compelling stories:**
- **The unmarried-young map.** Share of 25–34-year-olds never married has surged. County-level map is striking.
- **Births to unmarried mothers.** A long-running story with strong geographic patterns.
- **Cohabiting couples by metro.** Unmarried-partner households as a share of all households — urban/rural split.
- **Same-sex couple households.** Visible in PUMS via PARTNER and SEX.

**Our coverage:** None.

---

## 13. Veterans & military service ⚪

**Key ACS tables:** B21001 (veteran status by age × sex), B21002 (period of service), B21100 (service-connected disability), B21005 (employment by veteran status).

**PUMS variables:** MIL, MLPA–MLPK (period flags), VPS.

**Compelling stories:**
- **Veteran density by county.** Concentrations near bases (Hampton Roads, San Diego, Fayetteville NC) and in retiree-heavy regions.
- **Vietnam-era vs. post-9/11 vet share.** Generational replacement is moving fast — interesting age × geography story.
- **Service-connected disability rate.** Health legacy of recent conflicts.

**Our coverage:** None.

---

## 14. Group quarters & institutional population ⚪

**Key ACS tables:** B26001 (group quarters population), B26101 (institutionalized: correctional, nursing, juvenile, mental health), B26201 (non-institutionalized: college dorms, military barracks).

**Compelling stories:**
- **Prison-population counties.** Counties whose population is artificially inflated by a prison — a politically loaded but well-documented story.
- **Nursing-home concentration.** Counties with elderly populations skewed by long-term-care facilities.
- **College-town demographics.** Group-quarters share immediately reveals dorm-heavy places.

**Our coverage:** None. Niche but produces single-story, headline-grade maps.

---

## What PUMS unlocks that ACS can't

Once PUMS is fully ingested (we have 5 states; the full 50 is a few GB), the following cross-tabs — none of which appear in ACS tables — become possible:

- **Homeownership rate by income decile × age bracket × race.** The single most-requested housing chart we can't get from ACS.
- **Rent burden distribution by household type.** Single mothers vs. married couples vs. roommates, side by side.
- **Recent-mover characteristics.** Where movers came from, what they earn, how their housing differs from stayers.
- **Multi-job workers.** Their industries, hours, demographics.
- **Working from home by industry × commute distance.** What jobs people travel for vs. don't.
- **Field-of-degree vs. occupation.** Are humanities majors actually working in different jobs than engineers? Quantifiable from PUMS.
- **Within-metro inequality.** Income distribution at the metro level by race, by tenure, by age — Gini decompositions.

The trade-off: PUMS smallest geography is PUMA (~100k people), so neighborhood-level stories are out of reach. The ACS 5-year tract data is the right tool for those.

---

## Suggested category tree for the front-end

A topic-first navigation that maps onto everything above:

1. **People** — Population, age, sex, race & ethnicity, immigration & language, veterans
2. **Households & Families** — Composition, multigenerational, single parents, marriage, fertility
3. **Housing** — Tenure, value, rent, costs, vintage, structure, vacancy, mobility
4. **Money** — Household income, earnings, poverty, inequality, public assistance
5. **Work** — Employment, occupation, industry, hours, self-employment
6. **Getting Around** — Commute mode, time, departure, vehicles, work-from-home
7. **Education** — Attainment, enrollment, fields of study
8. **Health & Coverage** — Insurance type, uninsured rate, disability
9. **Connectivity** — Computers, internet, broadband
10. **Where People Move** — In/out migration, recent movers, length of residence

Each topic page should:
- Open with a national choropleth at the most natural geography (county for most, metro for housing-cost stories, tract for granular ones).
- Offer a small picker of 3–6 sub-questions per topic — the "compelling stories" bullets above are exactly the candidate list.
- Include the PUMS cross-tab variants where they unlock something the ACS table can't.
- Link to a CSV-download for every chart (Compass / Flourish workflow).

---

## Suggested ingestion priorities

Ranked by story-impact-per-byte, given what we already have:

1. **Health insurance (B27xxx, HICOV).** "% uninsured by county" is exactly the prompt that started this doc. High recognition, low ingestion cost.
2. **Cost burden (B25070, B25091, B25093).** Closes the housing story we've started.
3. **Commute (B08303, B08006) + means of transport.** Whole new topic, immediately produces multiple national maps.
4. **Migration (B07xxx, MIGSP).** Lets us tell "where Californians went" and "who's moving to Boise" stories.
5. **Poverty + Gini (B17001, B19083).** Fills the inequality gap in our income coverage.
6. **Foreign-born / language (B05002, B16001).** High-recognition demographic stories.
7. **Disability (B18101, S1810).** Already half-there via PUMS; ACS county tables would give us the granularity.
8. **Industry / occupation (C24010, C24030).** Big PUMS payoff once we have it.
9. **Internet / broadband (B28002, B28011).** Small table family, big single-chart impact.
10. **Group quarters (B26xxx).** Niche but produces headline-grade single charts.

Everything above is county-level public ACS data, ingestable through the same pattern as our existing B-table loaders. PUMS additions are mostly already in the file — we just need to widen the column projection in `pums_ingest.py` and re-pull.

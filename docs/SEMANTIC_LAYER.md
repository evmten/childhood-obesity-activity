```md
# Semantic Layer

## Grain & Keys
- **Grain:** 1 row = `COUNTRY × AGE × SEX × YEAR`
- **Keys:** `COUNTRY, AGE, SEX, YEAR` (not null; unique per row)

## Dimensions (conformed)
- `COUNTRY` – region/country code (e.g., AUT, BE-VLG, GB-ENG, …)
- `AGE` – {11, 13, 15}
- `SEX` – {MALE, FEMALE}
- `YEAR` – {2005, 2009} in this curated join (see scope note)

## Measures & Definitions
- `ACTIVITY_VAL` – Percent meeting physical activity guideline (0–100)
- `OBESITY_VAL` – Percent overweight (including obesity) (0–100)
- `GAP_pp` – `ACTIVITY_VAL − OBESITY_VAL` (percentage points)
- `TREND_Δ_2009_2005` – measure(2009) − measure(2005) after filtering to a single group or aggregated via simple average

**Aggregation defaults:** simple averages across selections (no weighting).

## Scope / Join Rule
- Curated table is an **inner join** of activity and obesity on `[COUNTRY, AGE, SEX, YEAR]`.
- This **excludes 2001 obesity-only** rows (no matching activity). Intentional for “apples-to-apples” comparisons.

## Data Quality Rules
- Keys not null; unique per row.
- Measures numeric; expected range `[0, 100]`.
- Sanity counts observed: activity=438, obesity=566, curated(inner)=438.

## Qlik Master Items (suggested)
**Master Dimensions:** COUNTRY, AGE, SEX, YEAR  
**Master Measures:**
- Avg Activity % → `Avg([ACTIVITY_VAL])`
- Avg Obesity % → `Avg([OBESITY_VAL])`
- Gap (pp) → `Avg([ACTIVITY_VAL]) - Avg([OBESITY_VAL])`
- Trend Δ (2009–2005) → `Only({<YEAR={2009}>} [ACTIVITY_VAL]) - Only({<YEAR={2005}>} [ACTIVITY_VAL])` (analogous for obesity)

> Note: If multiple rows are selected, ensure the set analysis yields a single value (`Only`) or define an explicit aggregation approach in the app.

## Known Limits
- No population weighting (simple averages by design).
- Regions/country codes are as provided by source (e.g., GB-ENG, GB-SCT, GB-WLS).
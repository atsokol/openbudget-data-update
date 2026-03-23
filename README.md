# Openbudget Data Update

An automated pipeline that downloads Ukrainian municipal budget data from the
[Open Budget](https://openbudget.gov.ua/en) resource using available [API](https://confluence-ext.spending.gov.ua/spaces/OpenBudget/pages/364490/10.+%D0%9F%D1%83%D0%B1%D0%BB%D1%96%D1%87%D0%BD%D0%B5+API), stores it as Parquet
files in this repository, and runs weekly via GitHub Actions.

---

## What this pipeline produces

| File | Description |
|---|---|
| `data/parquet/incomes.parquet` | Monthly income rows per city (general + special fund) |
| `data/parquet/expenses.parquet` | Monthly expenditure by economic classification |
| `data/parquet/expenses_functional.parquet` | Monthly expenditure by functional/programme classification (aggregated) |
| `data/parquet/debts.parquet` | Financing and debt data |
| `data/parquet/credits.parquet` | Budget crediting data |

All rows include a `CITY` column (human-readable city name) derived by joining
`COD_BUDGET` against `inputs/city_codes.csv`.

---

## Repository layout

```
.
├── inputs/
│   ├── city_codes.csv          # All historical + current budget codes → city name
│   ├── city_codes_current.csv  # Current active code per city (used for API requests)
│   ├── variable_types.csv      # API response columns + readr col_types per endpoint
│
├── data/
│   └── parquet/
│       └── *.parquet           # Output data files (committed to git)
│
├── src/
│   ├── helpers/                # Reusable functions, sourced by main scripts
│   │   ├── api.R               # API construction + HTTP retry logic
│   │   └── utils.R             # Shared IO helpers + resolve_code_conflicts()
│   │
│   ├── load_data.R             # One-time full historical load (2021 → present)
│   ├── update_data.R           # Incremental weekly update
│   ├── add_city.R              # Add a new city (backfills all years, all codes)
│   ├── remove_duplicates.R     # De-duplicate all parquet data files
│   │
│   └── tests/
│       ├── test_pipeline.R     # Comprehensive integrity test suite (~90 assertions)
│       ├── test_parquet.R      # Parquet read/write and IO helper checks
│       ├── test_api.R          # Manual API smoke-test for one code/year
│       └── test_city_codes.R   # Probe all city codes × all years against live API
│
└── .github/workflows/
    ├── import-data.yaml        # Manual trigger: full historical load
    ├── update-data.yaml        # Weekly schedule + manual trigger: incremental update
    ├── add-city.yaml           # Manual trigger: add a new city
    └── remove-duplicates.yaml  # Manual trigger: de-duplicate + repair
```

---

## Source file responsibilities

### `src/helpers/api.R` — API layer

- **`api_construct(budgetCode, budgetItem, classificationType, year)`**
  Builds the full URL for one API call. The endpoint is
  `https://api.openbudget.gov.ua/api/public/localBudgetData`.
  `classificationType` is only included for `EXPENSES` and `CREDITS`.

- **`call_api(api_path, col_types, max_retries = 3)`**
  Makes one HTTP GET, parses the semicolon-delimited response, and converts
  `REP_PERIOD` from `MM.YYYY` to the last day of that month.
  Retry behaviour:
  - Any HTTP error, connection failure, or empty response body retries up to
    `max_retries` times.
  - After a 429 (rate limit) the retry pause is 10 s; for other errors 3 s.
  - A 200 response with 0 rows is returned as-is (no retry — genuinely absent
    data).

- **`download_data(BUDGETCODE, YEAR)`**
  Iterates all endpoint × code × year combinations defined in
  `inputs/variable_types.csv`, calls `call_api()` for each, and returns a
  named list keyed by `"INCOMES"`, `"EXPENSES, ECONOMIC"`, `"EXPENSES, PROGRAM"`,
  `"FINANCING_DEBTS"`, `"CREDITS, CREDIT"`.

### `src/helpers/utils.R` — shared helpers

Sourced by `load_data.R`, `update_data.R`, and `add_city.R`.

| Function | Purpose |
|---|---|
| `safe_download_data(codes, years, max_retries)` | Wraps `download_data()` with a top-level retry loop for full-batch network failures |
| `map_api_response(api_data)` | Converts the raw named list to `list(credits, expenses, expenses_functional, debts, incomes)` |
| `resolve_code_conflicts(new_data, current_code)` | Resolves cross-code duplication for a city with multiple historical codes — see below |
| `read_data(path)` | Reads data from `data/parquet/`, preserving `COD_BUDGET` as character |
| `read_data_cols(path, columns)` | Reads selected columns from parquet |
| `write_data(df, path)` | Writes `df` to `data/parquet/<name>.parquet` |
| `clean_data(folder)` | De-duplicates all parquet files in `data/parquet/` |

### `src/load_data.R` — initial full load

Run once to populate `data/parquet/` from scratch. Downloads all data from 2021 to the
current year for every city in `inputs/city_codes_current.csv`, joins `CITY`
onto each row using `inputs/city_codes.csv` (all historical codes), and writes
Parquet files for all five datasets.

### `src/update_data.R` — incremental weekly update

Run weekly by the GitHub Actions schedule. Two-phase logic:

1. **Missing city check** — compares `COD_BUDGET` values present in
   the incomes data against `inputs/city_codes_current.csv`. If any current
   codes are absent, downloads their full history (2021 → now) and appends to
   all five files.

2. **Period update** — reads the latest `REP_PERIOD` from the incomes data and
   compares it to the last complete calendar month. Downloads any new years,
   removes overlapping periods from existing data, and appends the refreshed rows.

### `src/add_city.R` — add a new city

Run manually after adding a new city's codes to the input files (see workflow
below). Detects all cities in `inputs/city_codes.csv` that have incomplete year
coverage in the incomes data, then for each:

1. Downloads **all codes** for the city × all missing years.
2. Resolves duplicate records that arise when multiple codes return data for the
   same period (via `resolve_code_conflicts()`).
3. Appends to all five data files with a final dedup that ignores `COD_BUDGET`
   format variants (10-digit vs 11-digit).
4. Warns if any years are still missing after the run — a signal that additional
   historical codes may be needed in `inputs/city_codes.csv`.

### `src/remove_duplicates.R`

Thin main script: sources `helpers/utils.R` and calls `clean_data("data")`.
The de-duplication logic lives in `helpers/utils.R`.

---

## Input reference files

### `inputs/city_codes.csv`

Maps **all** budget codes (historical and current) to city names. Some cities
have multiple codes because the API changed codes over time, or because the API
accepts a 10-digit query code but returns an 11-digit code in the response:

| Situation | Example |
|---|---|
| Single active code | `Lviv, 1356300000` |
| Code changed after 2022 | `Kryvyi Rih` has `0457800000` (2021–2022) and `0457810000` (2023+) |
| 10/11-digit variants | Most cities have both a 10-digit and an 11-digit form |
| Oblast-level entity | `R_Vinnytsia, 0210000000` |

All codes must remain in the file so that historical rows are correctly matched
during the city join in all scripts.

### `inputs/city_codes_current.csv`

Contains exactly **one current code per city**, used as the API query code for
new downloads. This is the code passed to `safe_download_data()` in
`load_data.R` and `update_data.R`, and is the preferred code kept by
`resolve_code_conflicts()` when multiple codes return data for the same period.

### `inputs/variable_types.csv`

One row per column per API endpoint. The `colType` column provides readr type
codes (`c` = character, `d` = double, `i` = integer, `f` = factor) that
`download_data()` assembles into a `col_types` string for `read_delim()`.

---

## GitHub Actions workflows

| Workflow | Trigger | Script | Purpose |
|---|---|---|---|
| `import-data.yaml` | Manual | `load_data.R` | Full historical load from scratch |
| `update-data.yaml` | Weekly (Sun 10:00 UTC) + manual | `update_data.R` | Incremental update |
| `add-city.yaml` | Manual | `add_city.R` | Add a new city (backfills all years) |
| `remove-duplicates.yaml` | Manual | `remove_duplicates.R` | De-duplicate + sync parquet |

All workflows commit changed files in `data/parquet/` back to `main`. The `update-data`,
`add-city`, and `remove-duplicates` workflows rebase before pushing to handle
concurrent runs.

---

## Adding a new city

Cities may have multiple `COD_BUDGET` codes due to API code changes over time,
or because the API accepts a 10-digit code but returns an 11-digit variant in
responses. `add_city.R` handles both cases automatically.

**Step 1 — discover the codes.** Use
`src/tests/test_city_codes.R` (edit `codes` at the top) to probe which codes
return data for which years.

**Step 2 — update the input files.**

`inputs/city_codes.csv` — add one row per code (all historical + current):
```
Bila Tserkva,1052700000
Bila Tserkva,10527000000
```

`inputs/city_codes_current.csv` — add one row with the current query code only:
```
Bila Tserkva,1052700000
```

**Step 3 — run the script** (or trigger `add-city.yaml` on GitHub Actions):
```bash
cd /path/to/repo
Rscript -e 'source("src/add_city.R")'
```

The script detects cities with incomplete year coverage, downloads all their
codes × all missing years, resolves any duplicate records across codes, and
appends to all five data files. It warns if any years are still missing after
the run, which indicates that additional historical codes may be needed.

### `resolve_code_conflicts()` — how duplicate records are handled

When multiple codes return data for the same city and period, the pipeline
applies two deduplication steps:

1. **Period-level**: for any period where the current code has data, rows from
   historical codes for that same period are dropped.
2. **Format-level**: rows that are identical in every column except `COD_BUDGET`
   (e.g. a 10-digit and 11-digit variant of the same code) are collapsed to one
   row, keeping the current-code version.

The final write also deduplicates on all columns except `COD_BUDGET` as a
safety net.

---

## API notes

- **Base URL**: `https://api.openbudget.gov.ua/api/public/localBudgetData`
- **Response format**: semicolon-delimited text (not JSON)
- **Period parameter**: `MONTH` — returns one row per month; dates arrive as
  `MM.YYYY` and are stored as the last day of the month
- **Rate limits**: 429 responses are retried after a 10-second pause
- All amounts are in UAH (Ukrainian hryvnia)

---

## Running locally

```r
# Set working directory to the repo root, then:

# Full historical load (first time only)
source("src/load_data.R")

# Incremental update
source("src/update_data.R")

# Add a new city (after updating input files)
source("src/add_city.R")

# Recompute summary files
source("src/summarise_data.R")

# Run integrity tests
source("src/tests/test_pipeline.R")
```

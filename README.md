# Openbudget Data Update

An automated pipeline that downloads Ukrainian municipal budget data from the
[Open Budget API](https://api.openbudget.gov.ua), stores it as CSV and Parquet
files in this repository, and runs weekly via GitHub Actions.

---

## What this pipeline produces

| File | Description |
|---|---|
| `data/incomes.csv` | Monthly income rows per city (general + special fund) |
| `data/expenses.csv` | Monthly expenditure by economic classification |
| `data/expenses_functional.csv` | Monthly expenditure by functional/programme classification (aggregated) |
| `data/debts.csv` | Financing and debt data |
| `data/credits.csv` | Budget crediting data |

Every CSV has a matching Parquet file under `data/parquet/`.

All rows include a `CITY` column (human-readable city name) derived by joining
`COD_BUDGET` against `inputs/city_codes.csv`.

---

## Repository layout

```
.
├── inputs/
│   ├── city_codes.csv          # Budget codes → city name mapping
│   ├── variable_types.csv      # API response columns + readr col_types per endpoint
│   └── budget_categories.csv   # Code-range → category name (Tax, Opex, Transfers, …)
│
├── data/
│   ├── *.csv                   # Output data files (committed to git)
│   └── parquet/
│       └── *.parquet           # Parquet mirror of every CSV
│
├── src/
│   ├── helpers/                # Reusable functions, sourced by main scripts
│   │   ├── api.R               # API construction + HTTP retry logic
│   │   └── utils.R             # Shared IO helpers
│   │
│   ├── load_data.R             # One-time full historical load (2021 → present)
│   ├── update_data.R           # Incremental weekly update
│   ├── remove_duplicates.R     # De-duplicate all CSVs + sync parquet
│   ├── summarise_data.R        # Compute data_analysis.csv + transfer_share.csv
│   │
│   └── tests/
│       ├── test_pipeline.R     # Comprehensive integrity test suite (~80 assertions)
│       ├── test_parquet.R      # Parquet write/read + CSV parity checks
│       ├── test_api.R          # Manual API smoke-test for one code/year
│       └── test_city_codes.R   # Probe all city codes × all years against live API
│
└── .github/workflows/
    ├── import-data.yaml        # Manual trigger: full historical load
    ├── update-data.yaml        # Weekly schedule + manual trigger: incremental update
    ├── summarise-data.yaml     # Manual trigger: recompute summary files
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

Sourced by `load_data.R` and `update_data.R`.

| Function | Purpose |
|---|---|
| `safe_download_data(codes, years, max_retries)` | Wraps `download_data()` with a top-level retry loop for full-batch network failures |
| `map_api_response(api_data)` | Converts the raw named list to `list(credits, expenses, expenses_functional, debts, incomes)` |
| `read_data_csv(path)` | `read_csv` with `COD_BUDGET = col_character()` — prevents leading-zero loss on re-read |
| `write_data(df, csv_path)` | Writes `df` to both the CSV path and `data/parquet/<name>.parquet` atomically |
| `clean_csv_folder(folder)` | De-duplicates all CSVs in a folder and syncs their parquet counterparts |

### `src/load_data.R` — initial full load

Run once to populate `data/` from scratch. Downloads all data from 2021 to the
current year for every city in `inputs/city_codes.csv`, joins `CITY` onto each
row, and writes CSV + Parquet for all five datasets.

### `src/update_data.R` — incremental weekly update

Run weekly by the GitHub Actions schedule. Two-phase logic:

1. **Missing city check** — compares `COD_BUDGET` values present in
   `data/incomes.csv` against `inputs/city_codes.csv`. If any codes are absent,
   downloads their full history (2021 → now) and appends to all five files.

2. **Period update** — reads the latest `REP_PERIOD` from `incomes.csv` and
   compares it to the last complete calendar month. Downloads any new years,
   removes overlapping periods from existing data, and appends the refreshed rows.

### `src/remove_duplicates.R`

Thin main script: sources `helpers/utils.R` and calls `clean_csv_folder("data")`.
The de-duplication logic lives in `helpers/utils.R`.

---

## Input reference files

### `inputs/city_codes.csv`

Maps budget codes to city names. Some cities have two codes because the API
changed codes after 2022:

| Situation | Example |
|---|---|
| Single active code | `Lviv, 1356300000` |
| Code changed after 2022 | `Kryvyi Rih` has both `0457800000` (2021–2022) and `0457810000` (2023+) |
| Oblast-level entity | `R_Vinnytsia, 0210000000` |

Both codes must remain in the file so that historical rows are correctly matched
during the city join in `load_data.R` and `update_data.R`.

### `inputs/variable_types.csv`

One row per column per API endpoint. The `colType` column provides readr type
codes (`c` = character, `d` = double, `i` = integer, `f` = factor) that
`download_data()` assembles into a `col_types` string for `read_delim()`.

### `inputs/budget_categories.csv`

Defines consecutive numeric code ranges and the category label for each range,
separately for income codes (`CATEG = "INC"`) and expense codes (`CATEG = "EXP"`).
Used only by `summarise_data.R`.

---

## GitHub Actions workflows

| Workflow | Trigger | Script | Purpose |
|---|---|---|---|
| `import-data.yaml` | Manual | `load_data.R` | Full historical load from scratch |
| `update-data.yaml` | Weekly (Sun 10:00 UTC) + manual | `update_data.R` | Incremental update |
| `summarise-data.yaml` | Manual | `summarise_data.R` | Recompute summary files |
| `remove-duplicates.yaml` | Manual | `remove_duplicates.R` | De-duplicate + sync parquet |

All workflows commit changed files in `data/` back to `main`. The `update-data`
and `remove-duplicates` workflows rebase before pushing to handle concurrent
runs.

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

# Recompute summary files
source("src/summarise_data.R")

# Run integrity tests
source("src/tests/test_pipeline.R")
```

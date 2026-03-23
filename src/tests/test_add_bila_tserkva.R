# test_add_bila_tserkva.R
#
# Safe dry-run of the add_city workflow for Bila Tserkva (code 3210300000).
# Operates entirely on in-memory data — no files are written.
#
# Run from project root: Rscript -e 'source("src/tests/test_add_bila_tserkva.R")'

library(readr)
library(tidyr)
library(dplyr)
library(purrr)
library(lubridate)
library(httr)
library(arrow)

source("src/helpers/api.R")
source("src/helpers/utils.R")

options(readr.show_col_types = FALSE)

# ── Config ─────────────────────────────────────────────────────────────────────

NEW_CITY    <- "Bila Tserkva"
NEW_CODE    <- "1052700000"
current_date <- floor_date(Sys.Date(), "month") - days(1)
current_year <- year(current_date)
expected_years <- 2021:current_year

# ── Pass/fail tracking ─────────────────────────────────────────────────────────

pass <- 0L; fail <- 0L
check <- function(label, expr) {
  ok <- tryCatch(isTRUE(expr), error = function(e) FALSE)
  if (ok) { message(sprintf("  PASS  %s", label)); pass <<- pass + 1L }
  else    { message(sprintf("  FAIL  %s", label)); fail <<- fail + 1L }
}

# ── [1] API connectivity ───────────────────────────────────────────────────────

message("\n[1] API connectivity check")

url_check <- paste0(
  "https://api.openbudget.gov.ua/api/public/localBudgetData?",
  "budgetCode=", NEW_CODE,
  "&budgetItem=INCOMES&period=MONTH&year=", current_year
)
api_ok <- FALSE
for (.attempt in 1:3) {
  api_ok <- tryCatch({
    status_code(GET(url_check, timeout(30))) == 200
  }, error = function(e) FALSE)
  if (api_ok) break
  Sys.sleep(3)
}
rm(.attempt)

check(sprintf("API returns HTTP 200 for %s INCOMES %d", NEW_CODE, current_year), api_ok)

if (!api_ok) {
  message("API unreachable — aborting test.")
  quit(save = "no", status = 1)
}

# ── [2] Probe which years have data ───────────────────────────────────────────

message(sprintf("\n[2] Probing years %d–%d for code %s",
                min(expected_years), max(expected_years), NEW_CODE))

year_results <- map_int(expected_years, function(yr) {
  url <- paste0(
    "https://api.openbudget.gov.ua/api/public/localBudgetData?",
    "budgetCode=", NEW_CODE,
    "&budgetItem=INCOMES&period=MONTH&year=", yr
  )
  tryCatch({
    resp <- GET(url, timeout(30))
    if (status_code(resp) != 200) return(-1L)
    raw <- content(resp, as = "text", encoding = "UTF-8")
    if (nchar(raw) == 0) return(0L)
    nrow(read_delim(raw, delim = ";", show_col_types = FALSE))
  }, error = function(e) -1L)
})

names(year_results) <- as.character(expected_years)
message("  Rows per year (INCOMES):")
for (yr in names(year_results)) {
  n <- year_results[[yr]]
  status <- if (n > 0) sprintf("%d rows", n) else if (n == 0) "no data" else "error"
  message(sprintf("    %s: %s", yr, status))
}

years_with_data <- expected_years[year_results > 0]
check("at least one year has data", length(years_with_data) > 0)

if (length(years_with_data) == 0) {
  message("No data found for any year — aborting test.")
  quit(save = "no", status = 1)
}

# ── [3] Full download for years with data ─────────────────────────────────────

message(sprintf("\n[3] Full download for years: %s",
                paste(years_with_data, collapse = ", ")))

raw <- tryCatch(
  safe_download_data(NEW_CODE, years_with_data),
  error = function(e) { message(sprintf("  Download error: %s", e$message)); NULL }
)

check("download returned data", !is.null(raw) && length(raw) > 0)

if (is.null(raw)) {
  message("Download failed — aborting test.")
  quit(save = "no", status = 1)
}

data_map <- map_api_response(raw)

message("  Downloaded datasets:")
for (nm in names(data_map)) {
  n <- if (is.null(data_map[[nm]])) 0L else nrow(data_map[[nm]])
  message(sprintf("    %-25s %d rows", nm, n))
}

# ── [4] City join and resolve_code_conflicts ───────────────────────────────────

message("\n[4] City join and conflict resolution")

# Detect all COD_BUDGET values actually returned by the API for this city.
# Many cities have a 10-digit query code but the API responds with an 11-digit
# variant (same pattern as Kryvyi Rih, Poltava, Cherkasy, etc.).
codes_in_data <- data_map$incomes |>
  mutate(COD_BUDGET = as.character(COD_BUDGET)) |>
  pull(COD_BUDGET) |>
  unique() |>
  sort()

all_city_codes <- unique(c(NEW_CODE, codes_in_data))

if (!setequal(all_city_codes, NEW_CODE)) {
  extra <- setdiff(all_city_codes, NEW_CODE)
  message(sprintf(
    "  NOTE: API also returned COD_BUDGET variant(s): %s",
    paste(extra, collapse = ", ")
  ))
  message(sprintf(
    "  All codes to add to inputs/city_codes.csv: %s",
    paste(all_city_codes, collapse = ", ")
  ))
}

check("API returned only known code variants (10/11-digit)",
      all(nchar(all_city_codes) %in% c(10L, 11L)))

# Build temporary city_codes covering all returned code variants (in-memory only)
city_codes_real <- read_csv("inputs/city_codes.csv", show_col_types = FALSE)
city_codes_tmp  <- bind_rows(
  city_codes_real,
  tibble(city = NEW_CITY, value = all_city_codes)
)

join_and_resolve <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(NULL)
  df |>
    mutate(COD_BUDGET = as.character(COD_BUDGET)) |>
    left_join(city_codes_tmp, join_by(COD_BUDGET == value)) |>
    rename(CITY = city) |>
    resolve_code_conflicts(NEW_CODE)
}

incomes_new  <- join_and_resolve(data_map$incomes  |> distinct())
credits_new  <- join_and_resolve(data_map$credits  |> distinct())
expenses_new <- join_and_resolve(data_map$expenses |> distinct())
debts_new    <- join_and_resolve(data_map$debts    |> distinct())

ef_new <- NULL
if (!is.null(data_map$expenses_functional) &&
    nrow(data_map$expenses_functional) > 0) {
  ef_new <- aggregate_expenses_functional(data_map$expenses_functional) |>
    join_and_resolve()
}

# Checks
check("incomes: all rows joined to correct city",
      !is.null(incomes_new) &&
      all(incomes_new$CITY == NEW_CITY, na.rm = TRUE))
check("incomes: no NA CITY after join",
      !is.null(incomes_new) && !anyNA(incomes_new$CITY))
check("incomes: COD_BUDGET is character",
      !is.null(incomes_new) && is.character(incomes_new$COD_BUDGET))
check("incomes: all COD_BUDGET values are known city codes",
      !is.null(incomes_new) &&
      all(incomes_new$COD_BUDGET %in% all_city_codes))
check("incomes: REP_PERIOD is Date",
      !is.null(incomes_new) && inherits(incomes_new$REP_PERIOD, "Date"))
check("incomes: all REP_PERIOD are end-of-month",
      !is.null(incomes_new) &&
      all(day(incomes_new$REP_PERIOD) == days_in_month(incomes_new$REP_PERIOD)))

# ── [5] Year coverage check ───────────────────────────────────────────────────

message("\n[5] Year coverage")

if (!is.null(incomes_new)) {
  years_downloaded <- unique(year(incomes_new$REP_PERIOD))
  missing_years    <- setdiff(expected_years, years_downloaded)

  message(sprintf("  Years with downloaded income data: %s",
                  paste(sort(years_downloaded), collapse = ", ")))

  if (length(missing_years) > 0) {
    message(sprintf("  WARNING: years still missing: %s",
                    paste(missing_years, collapse = ", ")))
    message("  This may be expected if the API has no data for those years.")
    message("  If data should exist, additional historical codes may be needed.")
  } else {
    message(sprintf("  Complete coverage %d–%d confirmed.",
                    min(expected_years), max(expected_years)))
  }

  check("all expected years covered by downloaded data",
        length(missing_years) == 0)
}

# ── [6] Would-be additions summary ────────────────────────────────────────────

message("\n[6] Summary — rows that would be added to each data file")

datasets <- list(
  incomes             = incomes_new,
  expenses            = expenses_new,
  expenses_functional = ef_new,
  debts               = debts_new,
  credits             = credits_new
)
for (nm in names(datasets)) {
  n <- if (is.null(datasets[[nm]])) 0L else nrow(datasets[[nm]])
  message(sprintf("  %-25s %d rows", nm, n))
}

# Sample of the incomes data
if (!is.null(incomes_new) && nrow(incomes_new) > 0) {
  message("\n  Sample incomes rows (first 5):")
  print(head(incomes_new |> select(REP_PERIOD, FUND_TYP, COD_BUDGET, CITY, FAKT_AMT), 5),
        n = 5)
}

# ── Result ─────────────────────────────────────────────────────────────────────

message(sprintf("\n%d passed, %d failed", pass, fail))
if (fail > 0) {
  message("Some checks failed — review output above before running add_city.R.")
} else {
  message("All checks passed.")
  message(sprintf(
    "\nTo add %s, update inputs/city_codes.csv and inputs/city_codes_current.csv,",
    NEW_CITY
  ))
  message("then run: source(\"src/add_city.R\")")
}

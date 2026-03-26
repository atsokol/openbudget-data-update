library(readr)
library(tidyr)
library(dplyr)
library(purrr)
library(httr)
library(lubridate)
library(arrow)

source("src/helpers/api.R")
source("src/helpers/utils.R")

options(readr.show_col_types = FALSE)
dir.create("data/parquet", showWarnings = FALSE, recursive = TRUE)

# ── Validate input files ────────────────────────────────────────────────────────

required_input_files <- c(
  "inputs/city_codes.csv",
  "inputs/city_codes_current.csv",
  "inputs/variable_types.csv"
)
for (f in required_input_files) {
  if (!file.exists(f)) stop(sprintf("Required file missing: %s", f))
}
if (!data_file_exists("data/incomes.csv")) {
  stop("Required data file missing: data/incomes.csv (checked parquet too)")
}

city_codes         <- read_csv("inputs/city_codes.csv", show_col_types = FALSE)
city_codes_current <- read_csv("inputs/city_codes_current.csv", show_col_types = FALSE)

# ── Detect cities with incomplete year coverage ─────────────────────────────────

# Reference date: last complete calendar month (same formula as update_data.R)
current_date <- floor_date(Sys.Date(), "month") - days(1)
current_year <- year(current_date)
expected_years <- 2021:current_year

message("Checking city coverage in data...")

coverage <- read_data_cols("data/incomes.csv", c("CITY", "REP_PERIOD")) |>
  mutate(year = as.integer(year(REP_PERIOD))) |>
  distinct(CITY, year)

all_cities <- unique(city_codes$city)

cities_to_update <- map(all_cities, function(cty) {
  existing_years <- coverage |> filter(CITY == cty) |> pull(year)
  missing        <- setdiff(expected_years, existing_years)
  if (length(missing) > 0) tibble(city = cty, missing_years = list(as.integer(missing)))
  else NULL
}) |>
  bind_rows()

if (nrow(cities_to_update) == 0) {
  message("All cities have complete data. Nothing to do.")
  quit(save = "no", status = 0)
}

message(sprintf(
  "Found %d city/cities with incomplete coverage:",
  nrow(cities_to_update)
))
for (i in seq_len(nrow(cities_to_update))) {
  message(sprintf(
    "  %s — missing years: %s",
    cities_to_update$city[[i]],
    paste(cities_to_update$missing_years[[i]], collapse = ", ")
  ))
}

# ── Download and process data for each city with missing years ──────────────────

# Collect new rows across all cities, one data frame per dataset type
all_new <- list(credits = NULL, expenses = NULL, expenses_functional = NULL,
                expenses_functional_economic = NULL, debts = NULL, incomes = NULL)

for (i in seq_len(nrow(cities_to_update))) {
  cty           <- cities_to_update$city[[i]]
  missing_years <- cities_to_update$missing_years[[i]]

  # Validate: city must be in city_codes_current.csv before we can download
  current_code_row <- city_codes_current |> filter(city == cty)
  if (nrow(current_code_row) == 0) {
    warning(sprintf(
      "City '%s' not found in inputs/city_codes_current.csv — skipping.",
      cty
    ))
    next
  }
  current_code        <- current_code_row$value
  all_codes_for_city  <- city_codes |> filter(city == cty) |> pull(value)

  message(sprintf(
    "\nDownloading '%s' (%d code(s)) for years: %s",
    cty,
    length(all_codes_for_city),
    paste(missing_years, collapse = ", ")
  ))

  raw <- tryCatch(
    safe_download_data(all_codes_for_city, missing_years),
    error = function(e) {
      warning(sprintf("Failed to download data for '%s': %s", cty, e$message))
      NULL
    }
  )

  if (is.null(raw) || length(raw) == 0) {
    warning(sprintf("No data returned for '%s' — skipping.", cty))
    next
  }

  data_map <- map_api_response(raw)

  # Helper: join city name, resolve cross-code conflicts
  join_and_resolve <- function(df) {
    if (is.null(df) || nrow(df) == 0) return(NULL)
    df |>
      mutate(COD_BUDGET = as.character(COD_BUDGET)) |>
      left_join(city_codes, join_by(COD_BUDGET == value)) |>
      rename(CITY = city) |>
      resolve_code_conflicts(current_code)
  }

  ef_new     <- NULL
  ef_eco_new <- NULL
  if (!is.null(data_map$expenses_functional) && nrow(data_map$expenses_functional) > 0) {
    ef_new     <- aggregate_expenses_functional(data_map$expenses_functional) |>
      join_and_resolve()
    ef_eco_new <- disaggregate_expenses_functional(data_map$expenses_functional) |>
      join_and_resolve()
  }

  city_new <- list(
    credits                      = join_and_resolve(data_map$credits |> distinct()),
    expenses                     = join_and_resolve(data_map$expenses |> distinct()),
    expenses_functional          = ef_new,
    expenses_functional_economic = ef_eco_new,
    debts                        = join_and_resolve(data_map$debts |> distinct()),
    incomes                      = join_and_resolve(data_map$incomes |> distinct())
  )

  for (nm in names(all_new)) {
    all_new[[nm]] <- bind_rows(all_new[[nm]], city_new[[nm]])
  }
}

# Check whether any usable new data was collected
has_data <- any(map_lgl(all_new, ~ !is.null(.) && nrow(.) > 0))
if (!has_data) {
  stop("No new data was successfully downloaded. Check warnings above.")
}

# ── Append new rows to each data file ──────────────────────────────────────────

# Final dedup on all columns except COD_BUDGET: guards against same-value rows
# surviving under two different code formats after conflict resolution.
append_data <- function(file, new_data, description) {
  if (!data_file_exists(file)) stop(sprintf("Data file not found: %s", file))

  if (is.null(new_data) || nrow(new_data) == 0) {
    message(sprintf("Skipping %s: no new data", description))
    return(invisible(NULL))
  }

  message(sprintf("Appending to %s...", description))
  existing      <- read_data(file)
  original_cols <- names(existing)

  if (sum(is.na(new_data$CITY)) > 0) {
    warning(sprintf(
      "%s: %d rows with unmatched city codes (CITY = NA)",
      description, sum(is.na(new_data$CITY))
    ))
  }

  common_cols <- intersect(original_cols, names(new_data))
  if (length(common_cols) == 0) {
    stop(sprintf("%s: no common columns between existing and new data", description))
  }

  result <- bind_rows(
    existing |> select(all_of(common_cols)),
    new_data |> select(all_of(common_cols))
  ) |>
    arrange(REP_PERIOD, COD_BUDGET) |>
    distinct(across(-COD_BUDGET), .keep_all = TRUE)

  final_cols <- intersect(original_cols, names(result))
  result <- result |> select(all_of(final_cols))

  write_data(result, file)
  message(sprintf(
    "%s: %d total rows (%d new rows added)",
    description, nrow(result), nrow(result) - nrow(existing)
  ))
}

append_data("data/credits.csv",                          all_new$credits,                      "credits")
append_data("data/expenses.csv",                         all_new$expenses,                     "expenses")
append_data("data/expenses_functional.csv",              all_new$expenses_functional,          "expenses_functional")
append_data("data/expenses_functional_economic.csv",     all_new$expenses_functional_economic, "expenses_functional_economic")
append_data("data/debts.csv",                            all_new$debts,                        "debts")
append_data("data/incomes.csv",                          all_new$incomes,                      "incomes")

# ── Post-update coverage check ──────────────────────────────────────────────────
# Warn if any city still has missing years — may indicate historical codes were
# not included in inputs/city_codes.csv.

message("\nChecking coverage after update...")

post_coverage <- read_data_cols("data/incomes.csv", c("CITY", "REP_PERIOD")) |>
  mutate(year = as.integer(year(REP_PERIOD))) |>
  distinct(CITY, year)

any_still_missing <- FALSE
for (i in seq_len(nrow(cities_to_update))) {
  cty          <- cities_to_update$city[[i]]
  years_now    <- post_coverage |> filter(CITY == cty) |> pull(year)
  still_missing <- setdiff(expected_years, years_now)
  if (length(still_missing) > 0) {
    warning(sprintf(
      "City '%s' still missing years after update: %s. Check that all historical codes are listed in inputs/city_codes.csv.",
      cty, paste(still_missing, collapse = ", ")
    ))
    any_still_missing <- TRUE
  } else {
    message(sprintf("  %s: complete coverage confirmed", cty))
  }
}

if (any_still_missing) {
  message("\nSome cities still have incomplete coverage. See warnings above.")
} else {
  message("\nAll updated cities now have complete coverage.")
}

message("add_city completed.")

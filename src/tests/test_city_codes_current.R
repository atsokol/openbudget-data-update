# test_city_codes_current.R
#
# Probes city_codes_current.csv against the API for each year 2021–2026.
# Uses a single lightweight CREDITS (classificationType=CREDIT) call per code/year.
# Goal: verify coverage and surface any gaps.

library(readr)
library(dplyr)
library(httr)
library(tidyr)

city_codes <- read_csv("inputs/city_codes_current.csv", show_col_types = FALSE)
years <- 2021:2026

probe <- function(budget_code, year) {
  url <- paste0(
    "https://api.openbudget.gov.ua/api/public/localBudgetData?",
    "budgetCode=", budget_code,
    "&budgetItem=CREDITS",
    "&classificationType=CREDIT",
    "&period=MONTH",
    "&year=", year
  )
  tryCatch({
    resp <- GET(url, timeout(25))
    if (status_code(resp) != 200) return(NA_integer_)
    raw <- rawToChar(resp$content)
    if (nchar(trimws(raw)) == 0) return(0L)
    n_lines <- length(strsplit(trimws(raw), "\n")[[1]]) - 1L  # subtract header
    return(max(n_lines, 0L))
  }, error = function(e) NA_integer_)
}

# Build all combinations and probe
results <- expand_grid(city_codes, year = years) |>
  rename(code = value) |>
  rowwise() |>
  mutate(rows = {
    message(sprintf("  probing %-22s  code=%-15s  year=%d", city, code, year))
    Sys.sleep(0.2)
    probe(code, year)
  }) |>
  ungroup()

# ── Wide summary table ─────────────────────────────────────────────────────────

wide <- results |>
  mutate(status = case_when(
    is.na(rows) ~ "ERR",
    rows == 0   ~ "-",
    TRUE        ~ as.character(rows)
  )) |>
  select(city, code, year, status) |>
  pivot_wider(names_from = year, values_from = status, names_prefix = "Y")

message("\n── Results (row count, '-' = no data, ERR = API error) ──────────────────────")
print(wide, n = Inf, width = 140)

# ── Gaps: city × year combinations with no data ───────────────────────────────

gaps <- results |>
  filter(is.na(rows) | rows == 0) |>
  mutate(issue = if_else(is.na(rows), "API error", "no data")) |>
  select(city, code, year, issue)

if (nrow(gaps) > 0) {
  message("\n── GAPS (city × year combinations with no data or errors) ───────────────────")
  print(gaps, n = Inf)
} else {
  message("\n── No gaps: all city × year combinations returned data ───────────────────────")
}

# ── Cities missing entire years ────────────────────────────────────────────────

missing_years <- results |>
  filter(is.na(rows) | rows == 0) |>
  group_by(city, code) |>
  summarise(missing = paste(sort(year), collapse = ", "), n_missing = n(), .groups = "drop") |>
  arrange(desc(n_missing))

if (nrow(missing_years) > 0) {
  message("\n── Cities with missing years ─────────────────────────────────────────────────")
  print(missing_years, n = Inf)
}

# ── Fully dead codes ───────────────────────────────────────────────────────────

dead <- results |>
  group_by(city, code) |>
  summarise(any_data = any(!is.na(rows) & rows > 0), .groups = "drop") |>
  filter(!any_data)

if (nrow(dead) > 0) {
  message("\n── Codes with NO data in ANY year (candidates for removal) ──────────────────")
  print(dead)
} else {
  message("\n── All codes returned data in at least one year ──────────────────────────────")
}

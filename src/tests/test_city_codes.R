# test_city_codes.R
#
# For each budget code in city_codes.csv, probe the API for each year 2021-2025
# using a single lightweight INCOMES call. Records whether rows were returned.
# Goal: identify which codes can be removed (redundant or inactive).

library(readr)
library(dplyr)
library(httr)

city_codes <- read_csv("inputs/city_codes.csv", show_col_types = FALSE)
years      <- 2021:2025

probe <- function(budget_code, year) {
  url <- paste0(
    "https://api.openbudget.gov.ua/api/public/localBudgetData?",
    "budgetCode=", budget_code,
    "&budgetItem=INCOMES",
    "&period=MONTH",
    "&year=", year
  )
  tryCatch({
    resp <- GET(url, timeout(20))
    if (status_code(resp) != 200) return(NA_integer_)
    raw <- rawToChar(resp$content)
    if (nchar(trimws(raw)) == 0) return(0L)
    # Count data rows: split by newline, subtract header
    n_lines <- length(strsplit(trimws(raw), "\n")[[1]]) - 1L
    return(max(n_lines, 0L))
  }, error = function(e) NA_integer_)
}

# Build all combinations and probe
results <- tidyr::expand_grid(
  city_codes,
  year = years
) |>
  rename(code = value) |>
  rowwise() |>
  mutate(rows = {
    message(sprintf("  probing %-20s  code=%-15s  year=%d", city, code, year))
    Sys.sleep(0.15)   # be polite to the API
    probe(code, year)
  }) |>
  ungroup()

# ── Summary table: city × code, columns = years ────────────────────────────────

wide <- results |>
  mutate(has_data = case_when(
    is.na(rows) ~ "ERR",
    rows == 0   ~ "-",
    TRUE        ~ as.character(rows)
  )) |>
  select(-rows) |>
  tidyr::pivot_wider(names_from = year, values_from = has_data,
                     names_prefix = "Y")

message("\n── Results (row count per year, '-' = no data, ERR = API error) ──────────────")
print(wide, n = Inf, width = 120)

# ── Flag codes that are fully redundant ────────────────────────────────────────

active <- results |>
  filter(!is.na(rows), rows > 0) |>
  group_by(city, code) |>
  summarise(active_years = paste(sort(year), collapse = ", "), .groups = "drop")

# Per city: list all codes and their active years
message("\n── Active years per code ──────────────────────────────────────────────────────")
for (city_name in unique(active$city)) {
  rows <- active |> filter(city == city_name)
  message(sprintf("  %s", city_name))
  for (i in seq_len(nrow(rows))) {
    message(sprintf("    %-20s  → %s", rows$code[i], rows$active_years[i]))
  }
}

# Codes with zero active years (safe to remove)
dead_codes <- city_codes |>
  filter(!value %in% active$code)

if (nrow(dead_codes) > 0) {
  message("\n── Codes with NO data in any year (can be removed) ───────────────────────────")
  print(dead_codes)
} else {
  message("\n── All codes returned data in at least one year ──────────────────────────────")
}

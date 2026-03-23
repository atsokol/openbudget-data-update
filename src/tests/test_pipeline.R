# test_pipeline.R
#
# Comprehensive tests for the data update pipeline.
# Sections:
#   [1]  city_codes.csv integrity
#   [2]  CSV files — column presence & types
#   [3]  City coverage in each file
#   [4]  COD_BUDGET → CITY join integrity
#   [5]  Year/period coverage per city
#   [6]  Duplicate rows
#   [7]  REP_PERIOD format (end-of-month dates)
#   [8]  Code transition continuity (Kryvyi Rih, Poltava, Cherkasy)
#   [9]  update_data merge logic (no API)
#   [10] API smoke test (1 city, 1 year)
#   [11] resolve_code_conflicts() logic (no API, no files)

library(readr)
library(dplyr)
library(tidyr)
library(lubridate)
library(httr)
library(purrr)
library(arrow)

options(readr.show_col_types = FALSE)

# ── helpers ────────────────────────────────────────────────────────────────────

pass <- 0L; fail <- 0L; skip <- 0L

check <- function(label, expr) {
  ok <- tryCatch(isTRUE(expr), error = function(e) FALSE)
  if (ok) {
    message(sprintf("  PASS  %s", label)); pass <<- pass + 1L
  } else {
    message(sprintf("  FAIL  %s", label)); fail <<- fail + 1L
  }
}

skip_check <- function(label, reason) {
  message(sprintf("  SKIP  %s  [%s]", label, reason)); skip <<- skip + 1L
}

# ── load shared inputs ─────────────────────────────────────────────────────────

city_codes         <- read_csv("inputs/city_codes.csv")
city_codes_current <- read_csv("inputs/city_codes_current.csv")
var_types          <- read_csv("inputs/variable_types.csv")

data_files <- list(
  incomes              = "data/incomes.csv",
  expenses             = "data/expenses.csv",
  expenses_functional  = "data/expenses_functional.csv",
  debts                = "data/debts.csv",
  credits              = "data/credits.csv"
)

# Expected columns per budgetItem (from variable_types.csv, plus CITY added by pipeline)
expected_cols <- var_types |>
  mutate(budgetItem_cls = if_else(is.na(classificationType),
                                  budgetItem,
                                  paste(budgetItem, classificationType, sep = ", "))) |>
  group_by(budgetItem_cls) |>
  summarise(cols = list(c(variable, "CITY")), .groups = "drop")

file_to_item <- c(
  incomes             = "INCOMES",
  expenses            = "EXPENSES, ECONOMIC",
  expenses_functional = "EXPENSES, PROGRAM",
  debts               = "FINANCING_DEBTS",
  credits             = "CREDITS, CREDIT"
)

# ── [1] city_codes.csv integrity ───────────────────────────────────────────────

message("\n[1] city_codes.csv integrity")

check("no duplicate codes",
      nrow(city_codes) == n_distinct(city_codes$value))

check("all codes are character strings",
      is.character(city_codes$value))

check("no NA codes",
      !anyNA(city_codes$value))

check("no NA cities",
      !anyNA(city_codes$city))

check("code lengths are 10 or 11 digits",
      all(nchar(city_codes$value) %in% c(10L, 11L)))

# Each city has at most 3 codes (some have code transitions: old 10-digit,
# 11-digit transitional, new 10-digit). All cities carry both historical
# and current codes for backward-compatible joins.
check("no city has more than 3 codes",
      all(city_codes |> count(city) |> pull(n) <= 3))

# ── [1b] city_codes_current.csv integrity ──────────────────────────────────────

message("\n[1b] city_codes_current.csv integrity")

check("current: no duplicate codes",
      nrow(city_codes_current) == n_distinct(city_codes_current$value))

check("current: no NA codes",
      !anyNA(city_codes_current$value))

check("current: no NA cities",
      !anyNA(city_codes_current$city))

check("current: one code per city",
      nrow(city_codes_current) == n_distinct(city_codes_current$city))

check("current: all codes are a subset of city_codes",
      all(city_codes_current$value %in% city_codes$value))

# ── [2] CSV files — column presence & types ────────────────────────────────────

message("\n[2] Column presence & types")

for (nm in names(data_files)) {
  f <- data_files[[nm]]
  if (!file.exists(f)) {
    skip_check(sprintf("%-25s columns present", nm), "file not found"); next
  }
  df <- read_csv(f, col_types = cols(COD_BUDGET = col_character()))
  item_key <- file_to_item[[nm]]
  exp <- expected_cols |> filter(budgetItem_cls == item_key) |> pull(cols) |> unlist()

  # expenses_functional is filtered to aggregate EK rows (COD_CONS_EK == 0)
  # then COD_CONS_EK, COD_CONS_EK_NAME, and COD_CONS_MB_PK_NAME are dropped.
  # Check only surviving columns.
  if (nm == "expenses_functional") {
    exp <- intersect(exp, names(df))
  }

  missing_cols <- setdiff(exp, names(df))
  check(sprintf("%-25s all expected columns present", nm),
        length(missing_cols) == 0)
  if (length(missing_cols) > 0)
    message(sprintf("          missing: %s", paste(missing_cols, collapse = ", ")))

  check(sprintf("%-25s REP_PERIOD is Date", nm),
        inherits(df$REP_PERIOD, "Date"))

  check(sprintf("%-25s COD_BUDGET is character", nm),
        is.character(df$COD_BUDGET))

  check(sprintf("%-25s CITY column never NA", nm),
        !anyNA(df$CITY))

  check(sprintf("%-25s FAKT_AMT is numeric", nm),
        is.numeric(df$FAKT_AMT))
}

# ── [3] City coverage in each file ────────────────────────────────────────────

message("\n[3] City coverage")

expected_cities <- sort(unique(city_codes$city))

for (nm in names(data_files)) {
  f <- data_files[[nm]]
  if (!file.exists(f)) {
    skip_check(sprintf("%-25s cities present", nm), "file not found"); next
  }
  df    <- read_csv(f, col_types = cols_only(CITY = col_character()))
  found <- sort(unique(df$CITY))
  missing_cities <- setdiff(expected_cities, found)
  extra_cities   <- setdiff(found, expected_cities)

  check(sprintf("%-25s all expected cities present", nm),
        length(missing_cities) == 0)
  if (length(missing_cities) > 0)
    message(sprintf("          missing cities: %s", paste(missing_cities, collapse = ", ")))

  check(sprintf("%-25s no unexpected cities", nm),
        length(extra_cities) == 0)
  if (length(extra_cities) > 0)
    message(sprintf("          extra cities: %s", paste(extra_cities, collapse = ", ")))
}

# ── [4] COD_BUDGET → CITY join integrity ──────────────────────────────────────

message("\n[4] COD_BUDGET → CITY join integrity")

# All COD_BUDGET values must be from known codes (current OR historical).
# city_codes.csv contains both current and all historical codes.
all_known_codes <- city_codes$value

for (nm in names(data_files)) {
  f <- data_files[[nm]]
  if (!file.exists(f)) {
    skip_check(sprintf("%-25s COD_BUDGET in known codes", nm), "file not found")
    next
  }
  df <- read_csv(f, col_types = cols_only(COD_BUDGET = col_character()))
  unknown <- setdiff(unique(df$COD_BUDGET), all_known_codes)
  check(sprintf("%-25s all COD_BUDGET values are known codes", nm),
        length(unknown) == 0)
  if (length(unknown) > 0)
    message(sprintf("          unknown codes: %s", paste(unknown, collapse = ", ")))
}

# After city_codes reduction: old 11-digit codes should only appear in
# pre-2023 data (they are no longer in city_codes, but were used to download
# historical rows — these rows have CITY populated, so they are fine)
message("  INFO  Checking old codes only appear in historical data (pre-2023)...")
incomes_check <- read_csv("data/incomes.csv",
                          col_types = cols_only(COD_BUDGET = col_character(),
                                                REP_PERIOD = col_date()))
# Old 11-digit codes that should not appear in 2023+ data
# (Kyiv 26000000000 is excluded: both Kyiv codes cover all years,
#  so historical rows with 26000000000 in 2023+ are expected)
old_11digit_non_kyiv <- c(
  "23576000000","25559000000","24552000000","04576000000","09533000000",
  "20554000000","22564000000","16564000000","04578000000",
  "03551000000","13563000000","14549000000","15564000000","16570000000",
  "17564000000","18531000000","19549000000","07559000000","02536000000",
  "06552000000","08562000000","07507000000","02100000000"
)
old_in_2023_plus <- incomes_check |>
  filter(COD_BUDGET %in% old_11digit_non_kyiv, year(REP_PERIOD) >= 2023)
check("non-Kyiv old 11-digit codes absent from 2023+ data",
      nrow(old_in_2023_plus) == 0)
if (nrow(old_in_2023_plus) > 0)
  message(sprintf("          found %d rows with old codes in 2023+: %s",
                  nrow(old_in_2023_plus),
                  paste(unique(old_in_2023_plus$COD_BUDGET), collapse = ", ")))

# ── [5] Year/period coverage per city (incomes) ───────────────────────────────

message("\n[5] Year/period coverage (incomes)")

inc <- read_csv("data/incomes.csv",
                col_types = cols_only(CITY       = col_character(),
                                      REP_PERIOD = col_date(),
                                      FUND_TYP   = col_character()))

# Every city should have data in every year 2021-2024
coverage <- inc |>
  mutate(YEAR = year(REP_PERIOD)) |>
  distinct(CITY, YEAR) |>
  filter(YEAR %in% 2021:2024)

for (yr in 2021:2024) {
  cities_with_year <- coverage |> filter(YEAR == yr) |> pull(CITY)
  missing_this_year <- setdiff(expected_cities, cities_with_year)
  check(sprintf("all cities have %d data", yr),
        length(missing_this_year) == 0)
  if (length(missing_this_year) > 0)
    message(sprintf("          missing in %d: %s", yr,
                    paste(missing_this_year, collapse = ", ")))
}

# Every city should have all 12 months in each complete year (2021-2023)
monthly <- inc |>
  mutate(YEAR = year(REP_PERIOD)) |>
  filter(YEAR %in% 2021:2023, FUND_TYP == "T") |>
  distinct(CITY, YEAR, REP_PERIOD) |>
  count(CITY, YEAR)

incomplete <- monthly |> filter(n < 12)
check("all cities have 12 months in each complete year (2021-2023)",
      nrow(incomplete) == 0)
if (nrow(incomplete) > 0) {
  message("          incomplete year/city combos:")
  print(incomplete, n = Inf)
}

# ── [6] Duplicate rows ─────────────────────────────────────────────────────────

message("\n[6] Duplicate rows")

for (nm in names(data_files)) {
  f <- data_files[[nm]]
  if (!file.exists(f)) {
    skip_check(sprintf("%-25s no duplicates", nm), "file not found"); next
  }
  df <- read_csv(f, col_types = cols(COD_BUDGET = col_character()))
  check(sprintf("%-25s no duplicate rows", nm),
        nrow(df) == nrow(distinct(df)))
}

# ── [7] REP_PERIOD format (end-of-month) ──────────────────────────────────────

message("\n[7] REP_PERIOD format")

for (nm in names(data_files)) {
  f <- data_files[[nm]]
  if (!file.exists(f)) {
    skip_check(sprintf("%-25s end-of-month dates", nm), "file not found"); next
  }
  df <- read_csv(f, col_types = cols_only(REP_PERIOD = col_date()))
  # End-of-month: day(d) == days_in_month(d)
  bad_dates <- df |>
    filter(!is.na(REP_PERIOD)) |>
    filter(day(REP_PERIOD) != days_in_month(REP_PERIOD))
  check(sprintf("%-25s all REP_PERIOD are end-of-month", nm),
        nrow(bad_dates) == 0)
  if (nrow(bad_dates) > 0)
    message(sprintf("          %d non-end-of-month dates found", nrow(bad_dates)))
}

# ── [8] Code transition continuity ────────────────────────────────────────────

message("\n[8] Code transition continuity")

inc_full <- read_csv("data/incomes.csv",
                     col_types = cols_only(CITY       = col_character(),
                                           COD_BUDGET = col_character(),
                                           REP_PERIOD = col_date(),
                                           FUND_TYP   = col_character())) |>
  filter(FUND_TYP == "T") |>
  mutate(YEAR = year(REP_PERIOD))

# Kryvyi Rih: old codes cover 2021-2022; new code 0457810000 covers 2023+
kryvyi_old_years <- inc_full |>
  filter(COD_BUDGET %in% c("0457800000", "04578000000")) |>
  pull(YEAR) |> unique() |> sort()

kryvyi_new_years <- inc_full |>
  filter(COD_BUDGET == "0457810000") |>
  pull(YEAR) |> unique() |> sort()

check("Kryvyi Rih old codes (0457800000) have 2021-2022 data",
      all(c(2021, 2022) %in% kryvyi_old_years))
check("Kryvyi Rih new code (0457810000) has 2023+ data",
      any(kryvyi_new_years >= 2023))
check("Kryvyi Rih no gap: combined codes cover 2021-2024",
      all(2021:2024 %in% (inc_full |>
            filter(CITY == "Kryvyi Rih") |>
            pull(YEAR) |> unique())))

# Poltava: old codes cover 2021-2022; new code 1657010000 covers 2023+
poltava_old_years <- inc_full |>
  filter(COD_BUDGET %in% c("1657000000", "16570000000")) |>
  pull(YEAR) |> unique() |> sort()

poltava_new_years <- inc_full |>
  filter(COD_BUDGET == "1657010000") |>
  pull(YEAR) |> unique() |> sort()

check("Poltava old codes (1657000000) have 2021-2022 data",
      all(c(2021, 2022) %in% poltava_old_years))
check("Poltava new code (1657010000) has 2023+ data",
      any(poltava_new_years >= 2023))
check("Poltava no gap: combined codes cover 2021-2024",
      all(2021:2024 %in% (inc_full |>
            filter(CITY == "Poltava") |>
            pull(YEAR) |> unique())))

# Cherkasy: 23576000000 covers 2021; 2357600000 covers 2022+
cherkasy_old <- inc_full |>
  filter(COD_BUDGET == "23576000000") |>
  pull(YEAR) |> unique() |> sort()

cherkasy_new <- inc_full |>
  filter(COD_BUDGET == "2357600000") |>
  pull(YEAR) |> unique() |> sort()

check("Cherkasy old code (23576000000) has 2021 data",
      2021 %in% cherkasy_old)
check("Cherkasy new code (2357600000) has 2022+ data",
      any(cherkasy_new >= 2022))
check("Cherkasy no gap: combined codes cover 2021-2024",
      all(2021:2024 %in% (inc_full |>
            filter(CITY == "Cherkasy") |>
            pull(YEAR) |> unique())))

# ── [9] Update merge logic (no API) ───────────────────────────────────────────

message("\n[9] Update merge logic (simulation)")

# Simulate data_update(): existing data + new period, check overlap removal
sample_inc <- read_csv("data/incomes.csv",
                       col_types = cols_only(CITY       = col_character(),
                                             COD_BUDGET = col_character(),
                                             REP_PERIOD = col_date(),
                                             FUND_TYP   = col_character(),
                                             FAKT_AMT   = col_double()))

last_period  <- max(sample_inc$REP_PERIOD, na.rm = TRUE)
prior_period <- last_period %m-% months(1)

# Simulate "new download" = re-download of last_period with slightly changed values
simulated_new <- sample_inc |>
  filter(REP_PERIOD == last_period) |>
  mutate(FAKT_AMT = FAKT_AMT * 1.01)   # pretend updated figures

new_periods    <- unique(simulated_new$REP_PERIOD)
data_no_overlap <- sample_inc |> filter(!REP_PERIOD %in% new_periods)
common_cols     <- intersect(names(data_no_overlap), names(simulated_new))
merged          <- bind_rows(
  data_no_overlap |> select(all_of(common_cols)),
  simulated_new   |> select(all_of(common_cols))
) |> arrange(REP_PERIOD, COD_BUDGET)

check("merge: old period rows fully replaced (no old-period rows remain in merged)",
      nrow(merged |> filter(REP_PERIOD == last_period)) ==
      nrow(simulated_new))
check("merge: new FAKT_AMT values differ from old (update applied)",
      !isTRUE(all.equal(
        merged |> filter(REP_PERIOD == last_period) |> arrange(COD_BUDGET, FUND_TYP) |> pull(FAKT_AMT),
        sample_inc |> filter(REP_PERIOD == last_period) |> arrange(COD_BUDGET, FUND_TYP) |> pull(FAKT_AMT)
      )))
check("merge: prior periods untouched",
      nrow(merged |> filter(REP_PERIOD == prior_period)) ==
      nrow(sample_inc |> filter(REP_PERIOD == prior_period)))
check("merge: total rows = (existing - last_period) + new",
      nrow(merged) == nrow(data_no_overlap) + nrow(simulated_new))

# add_missing_data() simulation: add a fake new city, check it joins correctly
fake_city_code <- "9999999999"
fake_city_codes <- bind_rows(city_codes,
                             tibble(city = "TestCity", value = fake_city_code))
fake_new_data <- sample_inc |>
  filter(CITY == "Rivne", year(REP_PERIOD) == 2024) |>
  mutate(COD_BUDGET = fake_city_code) |>
  select(-CITY)

joined <- fake_new_data |>
  left_join(fake_city_codes, join_by(COD_BUDGET == value)) |>
  rename(CITY = city)

check("add_missing: join assigns CITY correctly",
      all(joined$CITY == "TestCity", na.rm = TRUE))
check("add_missing: no NA cities after join",
      !anyNA(joined$CITY))
check("add_missing: row count preserved through join",
      nrow(joined) == nrow(fake_new_data))

# ── [10] API smoke test (1 city, 1 year) ──────────────────────────────────────

message("\n[10] API smoke test (Rivne, current year)")

source("src/helpers/api.R")

current_year <- year(Sys.Date())
rivne_code   <- city_codes |> filter(city == "Rivne") |> pull(value)

api_ok <- tryCatch({
  url <- paste0(
    "https://api.openbudget.gov.ua/api/public/localBudgetData?",
    "budgetCode=", rivne_code[1],
    "&budgetItem=INCOMES&period=MONTH&year=", current_year
  )
  resp <- GET(url, timeout(20))
  status_code(resp) == 200
}, error = function(e) FALSE)

check("API reachable (HTTP 200 for Rivne INCOMES)", api_ok)

if (api_ok) {
  # Full download + join test for one city/year
  result <- tryCatch(
    download_data(rivne_code, current_year),
    error = function(e) { message(sprintf("  ERR   download_data: %s", e$message)); NULL }
  )

  check("download_data returns a list",           is.list(result))
  check("INCOMES key present in result",          "INCOMES" %in% names(result))

  if (!is.null(result) && "INCOMES" %in% names(result)) {
    inc_dl <- result[["INCOMES"]]
    check("downloaded data is a data frame",      is.data.frame(inc_dl))
    check("downloaded data has rows",             nrow(inc_dl) > 0)
    check("COD_BUDGET present",                   "COD_BUDGET" %in% names(inc_dl))
    check("REP_PERIOD present",                   "REP_PERIOD" %in% names(inc_dl))
    check("REP_PERIOD is Date",                   inherits(inc_dl$REP_PERIOD, "Date"))
    check("REP_PERIOD values are end-of-month",
          all(day(inc_dl$REP_PERIOD) == days_in_month(inc_dl$REP_PERIOD)))

    # Test join with city_codes
    joined_dl <- inc_dl |>
      mutate(COD_BUDGET = as.character(COD_BUDGET)) |>
      left_join(city_codes, join_by(COD_BUDGET == value)) |>
      rename(CITY = city)

    check("join produces CITY = Rivne for all rows",
          all(joined_dl$CITY == "Rivne", na.rm = TRUE))
    check("no NA cities after join",
          !anyNA(joined_dl$CITY))
    check("no extra cities in result",
          setequal(unique(joined_dl$CITY), "Rivne"))
  }
} else {
  for (lbl in c("download_data returns a list","INCOMES key present",
                "downloaded data is a data frame","downloaded data has rows",
                "COD_BUDGET present","REP_PERIOD present","REP_PERIOD is Date",
                "REP_PERIOD end-of-month","join CITY = Rivne","no NA cities",
                "no extra cities")) {
    skip_check(lbl, "API unreachable")
  }
}

# ── [11] resolve_code_conflicts() logic (no API, no files) ────────────────────

message("\n[11] resolve_code_conflicts() logic")

source("src/helpers/utils.R")

# Mock data: two codes for a hypothetical city
#   code_cur  — current/preferred code, has periods P2 and P3
#   code_old  — historical code, has periods P1 and P2 (overlap on P2)
p1 <- as.Date("2021-01-31")
p2 <- as.Date("2022-01-31")
p3 <- as.Date("2023-01-31")
code_cur <- "1000000001"
code_old <- "10000000010"  # same underlying code, 11-digit variant

mock_base <- tibble(
  REP_PERIOD = c(p2, p3, p1, p2),
  COD_BUDGET = c(code_cur, code_cur, code_old, code_old),
  CITY       = "MockCity",
  FAKT_AMT   = c(100, 200, 50, 110)  # p2: cur=100, old=110 (overlap)
)

resolved <- resolve_code_conflicts(mock_base, code_cur)

check("[11] overlap period: historical code row removed for p2",
      !any(resolved$COD_BUDGET == code_old & resolved$REP_PERIOD == p2))

check("[11] overlap period: current code row preserved for p2",
      any(resolved$COD_BUDGET == code_cur & resolved$REP_PERIOD == p2))

check("[11] historical-only period p1 preserved",
      any(resolved$COD_BUDGET == code_old & resolved$REP_PERIOD == p1))

check("[11] current-only period p3 preserved",
      any(resolved$COD_BUDGET == code_cur & resolved$REP_PERIOD == p3))

check("[11] total rows: 3 (p1-old, p2-cur, p3-cur)",
      nrow(resolved) == 3)

# Format duplicates: same values, different code format (e.g. 10 vs 11 digit)
mock_fmt <- tibble(
  REP_PERIOD = c(p1, p1),
  COD_BUDGET = c(code_cur, code_old),
  CITY       = "MockCity",
  FAKT_AMT   = c(300, 300)  # identical amounts — format duplicate
)
resolved_fmt <- resolve_code_conflicts(mock_fmt, code_cur)

check("[11] format-duplicate rows collapsed to one",
      nrow(resolved_fmt) == 1)

check("[11] format-duplicate: current code row retained",
      resolved_fmt$COD_BUDGET[[1]] == code_cur)

# Single-code city (no historical codes): output equals distinct(input)
mock_single <- tibble(
  REP_PERIOD = c(p1, p1, p2),       # p1 appears twice (exact duplicate)
  COD_BUDGET = code_cur,
  CITY       = "MockCity",
  FAKT_AMT   = c(10, 10, 20)
)
resolved_single <- resolve_code_conflicts(mock_single, code_cur)

check("[11] single-code: duplicate rows collapsed",
      nrow(resolved_single) == 2)

# Empty input
mock_empty <- mock_base[0, ]
resolved_empty <- resolve_code_conflicts(mock_empty, code_cur)

check("[11] empty input returns empty data frame",
      nrow(resolved_empty) == 0)

# ── summary ───────────────────────────────────────────────────────────────────

message(sprintf("\n%d passed, %d failed, %d skipped", pass, fail, skip))
if (fail > 0) stop(sprintf("%d test(s) failed", fail))

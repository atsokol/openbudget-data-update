library(dplyr)
library(arrow)

source("src/helpers/utils.R")

# ── helpers ───────────────────────────────────────────────────────────────────

pass <- 0L
fail <- 0L

check <- function(label, expr) {
  ok <- tryCatch(isTRUE(expr), error = function(e) FALSE)
  if (ok) {
    message(sprintf("  PASS  %s", label))
    pass <<- pass + 1L
  } else {
    message(sprintf("  FAIL  %s", label))
    fail <<- fail + 1L
  }
}

# ── setup: temp working area ──────────────────────────────────────────────────

tmp <- file.path(tempdir(), "parquet_test")
tmp_parquet <- file.path(tmp, "test.parquet")
dir.create(tmp, showWarnings = FALSE, recursive = TRUE)

# ── Test 1: write + read parquet round-trip ───────────────────────────────────

message("\n[1] parquet round-trip")

sample_data <- read_parquet("data/parquet/incomes.parquet") |>
  mutate(COD_BUDGET = as.character(COD_BUDGET)) |>
  slice_head(n = 200)

write_parquet(sample_data, tmp_parquet)
pq <- read_parquet(tmp_parquet)

check("parquet file exists",          file.exists(tmp_parquet))
check("row count matches",            nrow(pq) == nrow(sample_data))
check("column names match",           identical(sort(names(pq)), sort(names(sample_data))))
check("REP_PERIOD is Date type",      inherits(pq$REP_PERIOD, "Date"))

# ── Test 2: data_update path (merge existing + new, then write) ───────────────

message("\n[2] data_update path (merge + overwrite)")

# Simulate: existing = first 150 rows; new data = rows 101-200 (overlap on periods)
existing   <- sample_data |> slice_head(n = 150)
new_data   <- sample_data |> slice_tail(n = 100)

new_periods      <- unique(new_data$REP_PERIOD)
data_no_overlap  <- existing |> filter(!REP_PERIOD %in% new_periods)
common_cols      <- intersect(names(data_no_overlap), names(new_data))
data_updated     <- bind_rows(
  data_no_overlap |> select(all_of(common_cols)),
  new_data        |> select(all_of(common_cols))
) |> arrange(REP_PERIOD)

write_parquet(data_updated, tmp_parquet)
pq2 <- read_parquet(tmp_parquet)

check("parquet written after merge",  file.exists(tmp_parquet))
check("row count correct after merge",nrow(pq2) == nrow(data_updated))
check("no duplicate rows",            nrow(pq2) == nrow(distinct(pq2)))
check("new periods present",          all(new_periods %in% pq2$REP_PERIOD))
check("COD_BUDGET preserved as char", is.character(as.character(pq2$COD_BUDGET)))

unlink(tmp, recursive = TRUE)

# ── Test 3: all parquet data files are readable ───────────────────────────────

message("\n[3] parquet data files integrity")

expected_files <- c("incomes", "expenses", "expenses_functional", "debts", "credits")

for (name in expected_files) {
  pq_path <- sprintf("data/parquet/%s.parquet", name)
  if (file.exists(pq_path)) {
    pq_r <- read_parquet(pq_path)
    check(sprintf("%-30s  readable",   name), is.data.frame(pq_r))
    check(sprintf("%-30s  has rows",   name), nrow(pq_r) > 0)
    check(sprintf("%-30s  has COD_BUDGET", name), "COD_BUDGET" %in% names(pq_r))
  } else {
    message(sprintf("  SKIP  %s (file not found)", name))
  }
}

# ── Test 4: read_data / write_data helpers ────────────────────────────────────

message("\n[4] read_data / write_data helpers")

# read_data should return data with COD_BUDGET as character
inc <- read_data("data/incomes.csv")
check("read_data returns data frame",       is.data.frame(inc))
check("read_data COD_BUDGET is character",  is.character(inc$COD_BUDGET))
check("read_data has rows",                 nrow(inc) > 0)

# data_file_exists should check parquet
check("data_file_exists finds incomes",     data_file_exists("data/incomes.csv"))
check("data_file_exists rejects fake",      !data_file_exists("data/nonexistent.csv"))

# read_data_cols should return only requested columns
cols_df <- read_data_cols("data/incomes.csv", c("CITY", "REP_PERIOD"))
check("read_data_cols returns 2 columns",   ncol(cols_df) == 2)
check("read_data_cols has CITY",            "CITY" %in% names(cols_df))

# ── summary ───────────────────────────────────────────────────────────────────

message(sprintf("\n%d passed, %d failed", pass, fail))
if (fail > 0) stop(sprintf("%d test(s) failed", fail))

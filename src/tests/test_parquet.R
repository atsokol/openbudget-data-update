library(readr)
library(dplyr)
library(arrow)

options(readr.show_col_types = FALSE)

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
tmp_csv     <- file.path(tmp, "test.csv")
tmp_parquet <- file.path(tmp, "test.parquet")
dir.create(tmp, showWarnings = FALSE, recursive = TRUE)

# ── Test 1: save_data path (write_csv + write_parquet from in-memory data) ────

message("\n[1] save_data path")

sample_data <- read_csv("data/incomes.csv",
                        col_types = cols(COD_BUDGET = col_character())) |>
  slice_head(n = 200)

write_csv(sample_data, tmp_csv)
write_parquet(sample_data, tmp_parquet)

pq <- read_parquet(tmp_parquet)

check("parquet file exists",          file.exists(tmp_parquet))
check("row count matches",            nrow(pq) == nrow(sample_data))
check("column names match",           identical(sort(names(pq)), sort(names(sample_data))))
check("REP_PERIOD is Date type",      inherits(pq$REP_PERIOD, "Date"))
check("no rows lost vs CSV",          nrow(pq) == nrow(read_csv(tmp_csv)))

# ── Test 2: data_update path (merge existing + new, then write both) ──────────

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

write_csv(data_updated, tmp_csv)
write_parquet(data_updated, tmp_parquet)

pq2 <- read_parquet(tmp_parquet)

check("parquet written after merge",  file.exists(tmp_parquet))
check("row count correct after merge",nrow(pq2) == nrow(data_updated))
check("no duplicate rows",            nrow(pq2) == nrow(distinct(pq2)))
check("new periods present",          all(new_periods %in% pq2$REP_PERIOD))
check("COD_BUDGET preserved as char", is.character(pq2$COD_BUDGET))

unlink(tmp, recursive = TRUE)   # explicit cleanup (on.exit is unreliable in source())

# ── Test 3: all 6 real parquet files are readable and match their CSVs ────────

message("\n[3] real parquet files match CSVs")

csv_files <- list.files("data", pattern = "\\.csv$", full.names = TRUE)
pq_files  <- list.files("data/parquet", pattern = "\\.parquet$",
                         full.names = TRUE)

csv_df <- data.frame(
  csv  = csv_files,
  base = sub("\\.csv$", "", basename(csv_files)),
  stringsAsFactors = FALSE
)
pq_df <- data.frame(
  parquet = pq_files,
  base    = sub("\\.parquet$", "", basename(pq_files)),
  stringsAsFactors = FALSE
)
pairs <- merge(csv_df, pq_df, by = "base")

for (i in seq_len(nrow(pairs))) {
  base     <- pairs$base[i]
  csv_rows <- nrow(read_csv(pairs$csv[i]))
  pq_rows  <- nrow(read_parquet(pairs$parquet[i]))
  check(sprintf("%-30s  rows match (%d)", base, csv_rows), csv_rows == pq_rows)
}

# ── Test 4: summarise_data parquet outputs ────────────────────────────────────

message("\n[4] summarise_data outputs")

for (name in c("data_analysis", "transfer_share")) {
  pq_path  <- sprintf("data/parquet/%s.parquet", name)
  csv_path <- sprintf("data/%s.csv",             name)
  if (file.exists(pq_path) && file.exists(csv_path)) {
    pq_r  <- read_parquet(pq_path)
    csv_r <- read_csv(csv_path)
    check(sprintf("%-30s  parquet readable",   name), is.data.frame(pq_r))
    check(sprintf("%-30s  rows match CSV",     name), nrow(pq_r) == nrow(csv_r))
  } else {
    message(sprintf("  SKIP  %s (files not found — run summarise_data.R first)", name))
  }
}

# ── summary ───────────────────────────────────────────────────────────────────

message(sprintf("\n%d passed, %d failed", pass, fail))
if (fail > 0) stop(sprintf("%d test(s) failed", fail))

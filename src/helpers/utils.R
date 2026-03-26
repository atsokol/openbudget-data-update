# utils.R — shared helpers used by main scripts
#
# Requires the caller to have loaded: readr, dplyr, arrow
# and sourced: src/helpers/api.R  (for download_data())

# ── API ────────────────────────────────────────────────────────────────────────

# Wraps download_data() with a top-level retry loop for network failures.
# Individual API calls already retry inside call_api(); this handles the case
# where the entire batch fails (e.g., connection reset mid-download).
safe_download_data <- function(codes, years, max_retries = 3) {
  for (attempt in seq_len(max_retries)) {
    message(sprintf("Download attempt %d of %d", attempt, max_retries))
    result <- tryCatch(
      download_data(codes, years),
      error = function(e) {
        message(sprintf("Download failed (attempt %d): %s", attempt, e$message))
        if (attempt < max_retries) {
          message("Waiting 3 seconds before retry...")
          Sys.sleep(3)
        }
        NULL
      }
    )
    if (!is.null(result)) {
      message("Download successful")
      return(result)
    }
  }
  stop(sprintf("Failed to download data after %d attempts", max_retries))
}

# Convert raw API list (keyed by "INCOMES", "EXPENSES, ECONOMIC", etc.) to a
# named list with consistent short names.
map_api_response <- function(api_data) {
  get_or_null <- function(key) if (key %in% names(api_data)) api_data[[key]] else NULL
  list(
    credits             = get_or_null("CREDITS, CREDIT"),
    expenses            = get_or_null("EXPENSES, ECONOMIC"),
    expenses_functional = get_or_null("EXPENSES, PROGRAM"),
    debts               = get_or_null("FINANCING_DEBTS"),
    incomes             = get_or_null("INCOMES")
  )
}

# Resolve cross-code duplication for a single city's newly downloaded data.
#
# When a city has multiple historical codes, downloading all of them may yield:
#   (a) Overlap: both current and historical code return data for the same period.
#       → Drop historical-code rows for those periods; keep current-code rows.
#   (b) Format duplicates: the same underlying row appears under two code formats
#       (e.g. 10-digit vs 11-digit variant) with all other values identical.
#       → Deduplicate on all columns except COD_BUDGET, keeping current-code row.
#
# Arguments:
#   new_data     – data frame for one city (already city-joined), may contain
#                  rows from multiple COD_BUDGET values.
#   current_code – the single authoritative code from city_codes_current.csv.
resolve_code_conflicts <- function(new_data, current_code) {
  if (nrow(new_data) == 0) return(new_data)

  # Step 1: period-level conflict — for periods where current code has data,
  # drop all rows from historical codes.
  current_periods <- new_data |>
    dplyr::filter(COD_BUDGET == current_code) |>
    dplyr::pull(REP_PERIOD) |>
    unique()

  resolved <- new_data |>
    dplyr::filter(COD_BUDGET == current_code | !REP_PERIOD %in% current_periods)

  # Step 2: format-duplicate dedup — deduplicate on all columns except
  # COD_BUDGET, preferring the current-code row when two rows are identical
  # in every other column.
  non_cod_cols <- setdiff(names(resolved), "COD_BUDGET")
  resolved |>
    dplyr::arrange(dplyr::desc(COD_BUDGET == current_code)) |>
    dplyr::distinct(dplyr::across(dplyr::all_of(non_cod_cols)), .keep_all = TRUE)
}

# ── Aggregation ────────────────────────────────────────────────────────────────

# Aggregate raw PROGRAM-classification API data to one row per
# (period, fund_type, budget_code, FK, PK) — no economic breakdown.
#
# The API returns rows at the (PK, FK, EK) level. It also includes an aggregate
# row with COD_CONS_EK == 0 that already sums all detail EK rows.
# Keep only those aggregate rows and drop the now-constant EK columns.
# → output file: expenses_functional.parquet
aggregate_expenses_functional <- function(df) {
  df |>
    dplyr::filter(COD_CONS_EK == 0) |>
    dplyr::select(-COD_CONS_EK, -COD_CONS_EK_NAME) |>
    dplyr::distinct()
}

# Keep the EK breakdown rows (COD_CONS_EK != 0) for full economic detail.
# Drops the aggregate row (COD_CONS_EK == 0) to avoid double-counting.
# → output file: expenses_functional_economic.parquet
disaggregate_expenses_functional <- function(df) {
  df |>
    dplyr::filter(COD_CONS_EK != 0) |>
    dplyr::distinct()
}

# ── IO ─────────────────────────────────────────────────────────────────────────

# Resolve the parquet path that corresponds to a data path (given as "data/<name>.csv").
parquet_path_for <- function(path) {
  base <- sub("\\.csv$", "", basename(path))
  file.path("data/parquet", paste0(base, ".parquet"))
}

# Check whether a data file exists (parquet).
data_file_exists <- function(path) {
  file.exists(parquet_path_for(path))
}

# Read a data file from parquet.
# Preserves COD_BUDGET as character to prevent leading-zero loss.
read_data <- function(path) {
  pq <- parquet_path_for(path)
  if (!file.exists(pq)) stop(sprintf("Data file not found: %s", pq))
  df <- arrow::read_parquet(pq)
  if ("COD_BUDGET" %in% names(df))
    df <- dplyr::mutate(df, COD_BUDGET = as.character(COD_BUDGET))
  df
}

# Read only selected columns from a parquet data file.
read_data_cols <- function(path, columns) {
  pq <- parquet_path_for(path)
  if (!file.exists(pq)) stop(sprintf("Data file not found: %s", pq))
  arrow::read_parquet(pq, col_select = dplyr::all_of(columns))
}

# Write df to the parquet file under data/parquet/.
write_data <- function(df, path) {
  parquet_path <- parquet_path_for(path)
  if (!dir.exists(dirname(parquet_path))) dir.create(dirname(parquet_path), recursive = TRUE)
  arrow::write_parquet(df, parquet_path)
}

# De-duplicate all parquet files in the data/parquet/ folder.
clean_data <- function(folder = "data") {
  parquet_dir <- file.path(folder, "parquet")
  if (!dir.exists(parquet_dir)) {
    message("No parquet directory found in folder.")
    return(invisible(NULL))
  }
  parquet_files <- list.files(parquet_dir, pattern = "\\.parquet$", full.names = TRUE)
  if (length(parquet_files) == 0) {
    message("No parquet files found.")
    return(invisible(NULL))
  }
  for (pf in parquet_files) {
    message("Cleaning: ", basename(pf))
    df       <- arrow::read_parquet(pf) |>
      dplyr::mutate(COD_BUDGET = as.character(COD_BUDGET))
    df_clean <- dplyr::distinct(df)
    n_removed <- nrow(df) - nrow(df_clean)
    if (n_removed > 0) message(sprintf("  Removed %d duplicate rows", n_removed))
    arrow::write_parquet(df_clean, pf)
  }
  message("Cleaning complete.")
}

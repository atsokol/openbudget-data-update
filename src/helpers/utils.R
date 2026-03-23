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

# ── IO ─────────────────────────────────────────────────────────────────────────

# Read a data CSV, preserving COD_BUDGET as character to prevent leading-zero loss.
read_data_csv <- function(path) {
  readr::read_csv(path,
                  col_types = readr::cols(COD_BUDGET = readr::col_character()),
                  show_col_types = FALSE)
}

# Write df to both the CSV path and the matching parquet file under data/parquet/.
write_data <- function(df, csv_path) {
  readr::write_csv(df, csv_path)
  parquet_path <- file.path("data/parquet",
                            sub("\\.csv$", ".parquet", basename(csv_path)))
  arrow::write_parquet(df, parquet_path)
}

# De-duplicate all CSV files in a folder and sync their parquet counterparts.
clean_csv_folder <- function(folder) {
  folder <- normalizePath(folder, mustWork = TRUE)
  files  <- list.files(folder, pattern = "\\.csv$", full.names = TRUE)

  if (length(files) == 0) {
    message("No CSV files found in folder.")
    return(invisible(NULL))
  }

  for (file in files) {
    message("Cleaning: ", basename(file))

    # col_types prevents COD_BUDGET being read as double (would lose leading zeros)
    df       <- readr::read_csv(file,
                                col_types = readr::cols(COD_BUDGET = readr::col_character()),
                                show_col_types = FALSE)
    df_clean <- dplyr::distinct(df)

    n_removed <- nrow(df) - nrow(df_clean)
    if (n_removed > 0) message(sprintf("  Removed %d duplicate rows", n_removed))

    readr::write_csv(df_clean, file)

    parquet_dir <- file.path(folder, "parquet")
    if (dir.exists(parquet_dir)) {
      parquet_path <- file.path(parquet_dir,
                                sub("\\.csv$", ".parquet", basename(file)))
      arrow::write_parquet(df_clean, parquet_path)
    }
  }

  message("Cleaning complete.")
}

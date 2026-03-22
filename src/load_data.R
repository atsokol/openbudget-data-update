library(readr)
library(tidyr)
library(dplyr)
library(purrr)
library(httr)
library(lubridate)
library(arrow)

source("src/helpers/api.R")
source("src/helpers/utils.R")

# Suppress column specification messages
options(readr.show_col_types = FALSE)

# Ensure parquet output folder exists
dir.create("data/parquet", showWarnings = FALSE, recursive = TRUE)

# Validate input files exist
required_files <- c("inputs/city_codes.csv", "inputs/variable_types.csv")
for (f in required_files) {
  if (!file.exists(f)) {
    stop(sprintf("Required file missing: %s", f))
  }
}

city_codes <- read_csv("inputs/city_codes.csv", show_col_types = FALSE)

if (nrow(city_codes) == 0) {
  stop("city_codes.csv is empty")
}

cities <- city_codes |> pull(city) |> unique()
codes <- city_codes |> filter(city %in% cities) |> pull(value)

# Download data
current_year <- year(Sys.Date())
message(sprintf("Downloading data for years: %s", paste(2021:current_year, collapse = ", ")))
data <- safe_download_data(codes, seq(2021, current_year))

# Validate downloaded data
if (is.null(data) || length(data) == 0) {
  stop("Downloaded data is empty or NULL")
}

data_map <- map_api_response(data)

# Function to save data files
save_data <- function(data, file, description) {
  if (is.null(data) || nrow(data) == 0) {
    warning(sprintf("Skipping %s: no data available", description))
    return(invisible(NULL))
  }
  
  message(sprintf("Saving %s...", description))
  
  # Ensure COD_BUDGET is character type
  if ("COD_BUDGET" %in% names(data)) {
    data <- data |>
      mutate(COD_BUDGET = as.character(COD_BUDGET))
  }
  
  # Add city information
  data_with_city <- data |>
    left_join(city_codes, join_by(COD_BUDGET == value)) |>
    rename(CITY = city) |>
    distinct()
  
  # Validate join succeeded
  if (sum(is.na(data_with_city$CITY)) > 0) {
    warning(sprintf("%s: %d rows with unmatched city codes", 
                   description, sum(is.na(data_with_city$CITY))))
  }
  
  write_data(data_with_city, file)
  message(sprintf("%s: %d rows written", description, nrow(data_with_city)))
}

# Save each data file with validation
save_data(data_map$credits, "data/credits.csv", "credits")

save_data(data_map$expenses, "data/expenses.csv", "expenses")

# Expenses functional needs aggregation
if (!is.null(data_map$expenses_functional) && nrow(data_map$expenses_functional) > 0) {
  expenses_func_agg <- data_map$expenses_functional |>
    distinct() |>
    group_by(REP_PERIOD, FUND_TYP, COD_BUDGET,
             COD_CONS_MB_FK, COD_CONS_MB_FK_NAME, COD_CONS_MB_PK) |>
    summarise(across(where(is.numeric), ~sum(., na.rm = TRUE)), 
             .groups = "drop")
  save_data(expenses_func_agg, "data/expenses_functional.csv", "expenses_functional")
} else {
  warning("Skipping expenses_functional: no data available")
}

save_data(data_map$debts, "data/debts.csv", "debts")

save_data(data_map$incomes, "data/incomes.csv", "incomes")

message("Initial data load completed successfully")

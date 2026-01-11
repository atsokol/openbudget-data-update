library(readr)
library(tidyr)
library(dplyr)
library(purrr)
library(httr)
library(jsonlite)
library(lubridate)

source("download.R")

# Suppress column specification messages
options(readr.show_col_types = FALSE)

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

# FOR TESTING: Use only first city to speed up execution
# Comment out this line for production use
city_codes <- city_codes |> slice(1)
message(sprintf("TESTING MODE: Using only %s", city_codes$city[1]))

cities <- city_codes |> pull(city) |> unique()
codes <- city_codes |> filter(city %in% cities) |> pull(value)

# Helper function to safely download data with retry on connection reset
safe_download_data <- function(codes, periods, max_retries = 5) {
  for (attempt in seq_len(max_retries)) {
    result <- tryCatch(
      {
        message(sprintf("Download attempt %d of %d", attempt, max_retries))
        download_data(codes, periods)
      },
      error = function(e) {
        message(sprintf("Download failed (attempt %d): %s", attempt, e$message))
        if (attempt < max_retries) {
          wait_time <- min(2^attempt, 10)  # Exponential backoff, max 10s
          message(sprintf("Waiting %d seconds before retry...", wait_time))
          Sys.sleep(wait_time)
        }
        return(NULL)
      }
    )
    if (!is.null(result)) {
      message("Download successful")
      return(result)
    }
  }
  stop(sprintf("Failed to download data after %d attempts", max_retries))
}

# Download data
# Use last day of previous month (last complete month)
current_date <- floor_date(Sys.Date(), "month") - days(1)
current_year <- year(current_date)
current_month <- month(current_date)

# Check if data file exists
if (!file.exists("data/incomes.csv")) {
  stop("Data file not found: data/incomes.csv. Run initial data download first.")
}

# Get latest period from existing data
latest_period_data <- read_csv(
  "data/incomes.csv",
  col_types = cols_only(REP_PERIOD = col_date()),
  show_col_types = FALSE
)

if (nrow(latest_period_data) == 0 || all(is.na(latest_period_data$REP_PERIOD))) {
  stop("No valid dates found in incomes.csv")
}

latest_period <- latest_period_data |>
  pull(REP_PERIOD) |>
  max(na.rm = TRUE)
  
if (is.na(latest_period)) {
  stop("Could not determine latest period from data")
}

latest_year <- year(latest_period)
latest_month <- month(latest_period)

message(sprintf("Latest data: %s", format(latest_period, "%Y-%m-%d")))
message(sprintf("Current date: %s", format(current_date, "%Y-%m-%d")))

# If current period is the same as latest period, data is up to date
if (current_year == latest_year && current_month == latest_month) {
  message("Data is up to date.")
} else {
  # Determine years to update (API only allows by year)
  years_to_update <- integer(0)
  if (current_year > latest_year) {
    # New year: update for current year
    years_to_update <- c(years_to_update, current_year)
  }
  if (current_year == latest_year && current_month > latest_month) {
    # Same year, new months: update for current year
    years_to_update <- c(years_to_update, current_year)
  }
  if (latest_year == current_year - 1 && latest_month < 12) {
    # Previous year incomplete, update for previous year
    years_to_update <- c(years_to_update, latest_year)
  }

  if (length(years_to_update) > 0) {
    years_to_update <- sort(unique(years_to_update))
    message(sprintf("Updating data for years: %s", paste(years_to_update, collapse = ", ")))
    
    data <- safe_download_data(codes, years_to_update)

    # Validate downloaded data
    if (is.null(data) || length(data) == 0) {
      stop("Downloaded data is empty or NULL")
    }

    # Map data by name for safer access
    data_map <- list(
      credits = if ("CREDITS, CREDIT" %in% names(data)) data[["CREDITS, CREDIT"]] else NULL,
      expenses = if ("EXPENSES, ECONOMIC" %in% names(data)) data[["EXPENSES, ECONOMIC"]] else NULL,
      expenses_functional = if ("EXPENSES, PROGRAM" %in% names(data)) data[["EXPENSES, PROGRAM"]] else NULL,
      debts = if ("FINANCING_DEBTS" %in% names(data)) data[["FINANCING_DEBTS"]] else NULL,
      incomes = if ("INCOMES" %in% names(data)) data[["INCOMES"]] else NULL
    )

    # Function to update data files
    data_update <- function(file, new_data, description) {
      if (!file.exists(file)) {
        stop(sprintf("Data file not found: %s", file))
      }
      
      if (is.null(new_data) || nrow(new_data) == 0) {
        message(sprintf("Skipping %s: no new data", description))
        return(invisible(NULL))
      }
      
      message(sprintf("Updating %s...", description))
      
      existing_data <- read_csv(file, show_col_types = FALSE)
      
      # Ensure COD_BUDGET is character type in both datasets
      if ("COD_BUDGET" %in% names(existing_data)) {
        existing_data <- existing_data |>
          mutate(COD_BUDGET = as.character(COD_BUDGET))
      }
      
      if ("COD_BUDGET" %in% names(new_data)) {
        new_data <- new_data |>
          mutate(COD_BUDGET = as.character(COD_BUDGET))
      }
      
      # Add city information to new data
      new_data_with_city <- new_data |>
        left_join(city_codes, join_by(COD_BUDGET == value)) |>
        rename(CITY = city)
      
      # Validate join succeeded
      if (sum(is.na(new_data_with_city$CITY)) > 0) {
        warning(sprintf("%s: %d rows with unmatched city codes", 
                       description, sum(is.na(new_data_with_city$CITY))))
      }

      # Remove overlapping periods from existing data
      new_periods <- unique(new_data_with_city$REP_PERIOD)
      data_no_overlap <- existing_data |>
        filter(!REP_PERIOD %in% new_periods)

      # Ensure column compatibility before combining
      common_cols <- intersect(names(data_no_overlap), names(new_data_with_city))
      
      if (length(common_cols) == 0) {
        stop(sprintf("%s: No common columns between old and new data", description))
      }
      
      # Use bind_rows for safer combining (handles column differences)
      data_updated <- bind_rows(
        data_no_overlap |> select(all_of(common_cols)),
        new_data_with_city |> select(all_of(common_cols))
      ) |>
        arrange(REP_PERIOD, COD_BUDGET)

      # Validate result
      if (nrow(data_updated) == 0) {
        warning(sprintf("%s: Updated data is empty", description))
        return(invisible(NULL))
      }

      write_csv(data_updated, file)
      message(sprintf("%s: %d rows written (%d new periods)", 
                     description, nrow(data_updated), length(new_periods)))
    }

    # Update each data file with validation
    data_update("data/credits.csv", 
                data_map$credits |> distinct(), 
                "credits")
    
    data_update("data/expenses.csv", 
                data_map$expenses |> distinct(), 
                "expenses")
    
    data_update("data/expenses_functional.csv",
                data_map$expenses_functional |>
                  distinct() |> 
                  group_by(REP_PERIOD, FUND_TYP, COD_BUDGET,
                           COD_CONS_MB_FK, COD_CONS_MB_FK_NAME, COD_CONS_MB_PK) |>
                  summarise(across(where(is.numeric), ~sum(., na.rm = TRUE)), 
                           .groups = "drop"),
                "expenses_functional")
    
    data_update("data/debts.csv", 
                data_map$debts |> distinct(), 
                "debts")
    
    data_update("data/incomes.csv", 
                data_map$incomes |> distinct(), 
                "incomes")
    
    message("Data update completed successfully")
  } else {
    message("Data is up to date.")
  }
}


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
city_codes_current <- read_csv("inputs/city_codes_current.csv", show_col_types = FALSE)

if (nrow(city_codes_current) == 0) {
  stop("city_codes_current.csv is empty")
}

# FOR TESTING: Uncomment to use only one city to speed up execution
# if (Sys.getenv("TEST_MODE") == "true") {
#   city_codes_current <- city_codes_current |> filter(city == "R_Rivne")
#   message(sprintf("TESTING MODE: Using only %s", city_codes_current$city[1]))
# }

# Use current codes for API requests; all historical codes for the CITY join
codes <- city_codes_current |> pull(value)

# Download data
# Use last day of previous month (last complete month)
current_date <- floor_date(Sys.Date(), "month") - days(1)
current_year <- year(current_date)
current_month <- month(current_date)

# Check if data file exists
if (!file.exists("data/incomes.csv")) {
  stop("Data file not found: data/incomes.csv. Run initial data download first.")
}

# Get latest period from existing data (more efficient - read only date column)
latest_period_data <- read_csv(
  "data/incomes.csv",
  col_types = cols_only(REP_PERIOD = col_date()),
  show_col_types = FALSE,
  lazy = FALSE
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

# Check for missing cities
message("Checking for missing cities...")
existing_codes_data <- read_csv(
  "data/incomes.csv",
  col_types = cols_only(COD_BUDGET = col_character()),
  show_col_types = FALSE,
  lazy = FALSE
)

existing_codes <- existing_codes_data |>
  pull(COD_BUDGET) |>
  unique() |>
  na.omit()

expected_codes <- city_codes_current |> pull(value) |> unique()
missing_codes <- setdiff(expected_codes, existing_codes)

if (length(missing_codes) > 0) {
  # Get city names for missing codes
  missing_cities_info <- city_codes |>
    filter(value %in% missing_codes) |>
    distinct(city, value)
  
  message(sprintf("Found %d missing budget codes for %d cities:", 
                  length(missing_codes),
                  n_distinct(missing_cities_info$city)))
  message(paste(sprintf("  %s (%s)", missing_cities_info$city, missing_cities_info$value), 
                collapse = "\n"))
  
  # Download all years of data for missing cities (2021 to current year)
  years_for_missing <- 2021:current_year
  message(sprintf("Downloading data for missing cities for years: %s", 
                  paste(years_for_missing, collapse = ", ")))
  
  missing_data <- tryCatch(
    {
      safe_download_data(missing_codes, years_for_missing)
    },
    error = function(e) {
      warning(sprintf("Failed to download data for missing cities: %s", e$message))
      return(NULL)
    }
  )
  
  # Validate downloaded data
  if (is.null(missing_data) || length(missing_data) == 0) {
    warning("Failed to download data for missing cities - they may not have data available")
  } else {
    missing_data_map <- map_api_response(missing_data)
    
    # Function to add missing city data
    add_missing_data <- function(file, new_data, description) {
      if (!file.exists(file)) {
        stop(sprintf("Data file not found: %s", file))
      }
      
      if (is.null(new_data) || nrow(new_data) == 0) {
        message(sprintf("Skipping %s: no data for missing cities", description))
        return(invisible(NULL))
      }
      
      message(sprintf("Adding missing cities to %s...", description))
      
      existing_data <- read_data_csv(file)

      # Store original column order from existing data
      original_cols <- names(existing_data)

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

      # Only keep columns that exist in both datasets
      common_cols <- intersect(names(existing_data), names(new_data_with_city))

      if (length(common_cols) == 0) {
        stop(sprintf("%s: No common columns between old and new data", description))
      }

      # Use bind_rows for safer combining
      data_updated <- bind_rows(
        existing_data |> select(all_of(common_cols)),
        new_data_with_city |> select(all_of(common_cols))
      ) |>
        arrange(REP_PERIOD, COD_BUDGET)

      # Validate result
      if (nrow(data_updated) == 0) {
        warning(sprintf("%s: Updated data is empty", description))
        return(invisible(NULL))
      }

      # Reorder columns to match original file structure
      final_cols <- intersect(original_cols, names(data_updated))
      data_updated <- data_updated |> select(all_of(final_cols))

      write_data(data_updated, file)
      message(sprintf("%s: %d rows written (added %d rows for missing cities)",
                     description, nrow(data_updated), nrow(new_data_with_city)))
    }
    
    # Add missing city data to each file
    add_missing_data("data/credits.csv", 
                     missing_data_map$credits |> distinct(), 
                     "credits")
    
    add_missing_data("data/expenses.csv", 
                     missing_data_map$expenses |> distinct(), 
                     "expenses")
    
    add_missing_data("data/expenses_functional.csv",
                     aggregate_expenses_functional(missing_data_map$expenses_functional),
                     "expenses_functional")
    
    add_missing_data("data/debts.csv", 
                     missing_data_map$debts |> distinct(), 
                     "debts")
    
    add_missing_data("data/incomes.csv", 
                     missing_data_map$incomes |> distinct(), 
                     "incomes")
    
    message("Missing cities data added successfully")
  }
} else {
  message("All cities are present in the data")
}

# Regular update for all cities
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

    data_map <- map_api_response(data)

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
      
      existing_data <- read_data_csv(file)

      # Store original column order from existing data
      original_cols <- names(existing_data)

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

      # Only keep columns that exist in both datasets to avoid column mismatch
      common_cols <- intersect(names(data_no_overlap), names(new_data_with_city))

      if (length(common_cols) == 0) {
        stop(sprintf("%s: No common columns between old and new data", description))
      }

      # Verify all original columns are present
      missing_cols <- setdiff(original_cols, common_cols)
      if (length(missing_cols) > 0) {
        warning(sprintf("%s: New data missing columns: %s",
                       description, paste(missing_cols, collapse = ", ")))
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

      # Reorder columns to match original file structure
      final_cols <- intersect(original_cols, names(data_updated))
      data_updated <- data_updated |> select(all_of(final_cols))

      write_data(data_updated, file)
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
                aggregate_expenses_functional(data_map$expenses_functional),
                "expenses_functional")
    
    data_update("data/debts.csv", 
                data_map$debts |> distinct(), 
                "debts")
    
    data_update("data/incomes.csv", 
                data_map$incomes |> distinct(), 
                "incomes")
    
    message("Regular data update completed successfully")
  } else {
    message("Data is up to date.")
  }
}


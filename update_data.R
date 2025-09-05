library(readr)
library(tidyr)
library(dplyr)
library(purrr)
library(httr)
library(jsonlite)
library(lubridate)

source("download.R")

city_codes <- read_csv("inputs/city_codes.csv")

cities <- city_codes |> pull(city) |> unique()
codes <- city_codes |> filter(city %in% cities) |> pull(value)

# Helper function to safely download data with retry on connection reset
safe_download_data <- function(codes, periods, max_retries = 5) {
  attempt <- 1
  while (attempt <= max_retries) {
    result <- tryCatch(
      {
        download_data(codes, periods)
      },
      error = function(e) {
          attempt <- attempt + 1
          Sys.sleep(2) # brief pause before retry
          return(NULL)
      }
    )
    if (!is.null(result)) return(result)
  }
  stop("Failed to download data.")
}

# Download data
current_date <- Sys.Date()
current_year <- year(current_date)
current_month <- month(current_date)

latest_period <- read_csv(
  "data/incomes.csv",
  col_types = cols_only(REP_PERIOD = col_date())
  ) |>
  pull(REP_PERIOD) |>
  max()
latest_year <- year(latest_period)
latest_month <- month(latest_period)

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
    data <- safe_download_data(codes, years_to_update)

    # Function to update data files
    data_update <- function(file, data) {
      existing_data <- read_csv(file)

      new_data <- data |>
        left_join(city_codes, join_by(COD_BUDGET == value)) |>
        rename(CITY = city)

      data_no_overlap <- existing_data |>
        filter(!REP_PERIOD %in% unique(new_data$REP_PERIOD))

      data_updated <- rbind(data_no_overlap, new_data)

      write_csv(data_updated, file)
    }

    data_update("data/credits.csv", data[[1]])
    data_update("data/expenses.csv", data[[2]])
    data_update("data/expenses_functional.csv",
                data[[3]] |>
                  group_by(REP_PERIOD, FUND_TYP, COD_BUDGET,
                           COD_CONS_MB_FK, COD_CONS_MB_FK_NAME) |>
                  summarise_if(is.numeric, sum, na.rm = TRUE))
    data_update("data/debts.csv", data[[4]])
    data_update("data/incomes.csv", data[[5]])
  } else {
    message("Data is up to date.")
  }
}


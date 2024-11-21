library(readr)
library(tidyr)
library(dplyr)
library(purrr)
library(httr)
library(jsonlite)
library(lubridate)

source("download.R")

city_codes <- read_csv("inputs/city_codes.csv")

cities <- city_codes |> filter(city != "R_Vinnitsia") |> pull(city) |> unique()
codes <- city_codes |> filter(city %in% cities) |> pull(value)

# Download data
current_year <- year(Sys.Date())
latest_period <- read_csv(
  "data/incomes.csv",
  col_types = cols_only(REP_PERIOD = col_date())
  ) |>
  pull(REP_PERIOD) |>
  max() |>
  year()

data <- download_data(codes, seq(latest_period, current_year))

# Function to update data files
data_update <- function(file, dat) {
  existing_data <- read_csv(file)

  new_data <- dat |>
    left_join(city_codes, join_by(COD_BUDGET == value)) |>
    rename(CITY = city)

  data_no_overlap <- existing_data |>
    filter(!REP_PERIOD %in% new_data$REP_PERIOD) |>
    filter(!COD_BUDGET %in% new_data$COD_BUDGET)

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


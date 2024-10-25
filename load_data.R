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
data <- download_data(codes, seq(2021, current_year))

write_csv(data[[1]] |> left_join(city_codes, join_by(COD_BUDGET == value)) |> rename(CITY = city),
          "data/credits.csv")
write_csv(data[[2]] |> left_join(city_codes, join_by(COD_BUDGET == value)) |> rename(CITY = city),
          "data/expenses.csv")
write_csv(data[[3]] |>
      group_by(REP_PERIOD, FUND_TYP, COD_BUDGET, COD_CONS_MB_FK, COD_CONS_MB_FK_NAME) |>
      summarise_if(is.numeric, sum, na.rm = TRUE) |>
      left_join(city_codes, join_by(COD_BUDGET == value)) |>
      rename(CITY = city),
      "data/expenses_functional.csv"
)
write_csv(data[[4]] |> left_join(city_codes, join_by(COD_BUDGET == value)) |> rename(CITY = city),
          "data/debts.csv")
write_csv(data[[5]] |> left_join(city_codes, join_by(COD_BUDGET == value)) |> rename(CITY = city),
          "data/incomes.csv")

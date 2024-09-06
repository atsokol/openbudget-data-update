library(readr)
library(tidyr)
library(dplyr)
library(purrr)
library(httr)
library(jsonlite)

source("download.R")

city_codes <- read_csv("inputs/city_codes.csv")

cities <- c("Lviv", "Kyiv")
codes <- city_codes |> filter(city %in% cities) |> pull(value)

# Download data
data <- download_data(codes, seq(2021,2024))

write_csv(data[[1]], "data/credits.csv")
write_csv(data[[2]], "data/expenses.csv")
write_csv(data[[3]], "data/debts.csv")
write_csv(data[[4]], "data/incomes.csv")

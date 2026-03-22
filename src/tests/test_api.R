library(readr)
library(dplyr)
library(httr)
library(purrr)
library(lubridate)

source("src/helpers/api.R")

# Suppress column specification messages
options(readr.show_col_types = FALSE)

# Test with 1 code for 1 year
test_code <- "0457800000"  
test_year <- 2025

message("Testing API calls for:")
message(sprintf("  Budget code: %s", test_code))
message(sprintf("  Year: %s", test_year))
message("")

# Read variable types to understand what API calls are needed
var_types <- read_csv("inputs/variable_types.csv")

# Create all API paths for this code and year
test_apis <- var_types |>
  group_by(budgetItem, classificationType) |>
  summarise(col_type = paste(colType, collapse = ""), .groups = "drop") |>
  mutate(
    budgetCode = test_code,
    year = test_year
  ) |>
  rowwise() |>
  mutate(api_path = api_construct(budgetCode, budgetItem, classificationType, year)) |>
  ungroup()

# Display the API paths
message(sprintf("Total API calls needed: %d", nrow(test_apis)))
message("")
message("API paths:")
for (i in 1:nrow(test_apis)) {
  message(sprintf("\n%d. %s - %s", i, 
                  test_apis$budgetItem[i], 
                  ifelse(is.na(test_apis$classificationType[i]), 
                         "(no classification)", 
                         test_apis$classificationType[i])))
  message(sprintf("   %s", test_apis$api_path[i]))
}

# Execute the download
message("\n\nExecuting API calls...")
test_data <- tryCatch(
  {
    download_data(test_code, test_year)
  },
  error = function(e) {
    warning(sprintf("Failed to download data: %s", e$message))
    return(NULL)
  }
)

# Show what was downloaded
if (is.null(test_data) || length(test_data) == 0) {
  message('\nNo data downloaded - the API may not have data for this code/year combination')
} else {
  message(sprintf('\nDownloaded %d data categories:', length(test_data)))
  for (name in names(test_data)) {
    if (!is.null(test_data[[name]])) {
      message(sprintf('  - %s: %d rows', name, nrow(test_data[[name]])))
    } else {
      message(sprintf('  - %s: NULL (no data)', name))
    }
  }
}

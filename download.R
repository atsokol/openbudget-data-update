# Functions to download data from Open Budget portal

city_chooser <- function(City) {

  vec <- city_codes |>
    filter(city == City) |>
    unlist(use.names = FALSE)

  return(vec)
}


api_construct <- function(budgetCode,
                          budgetItem, # "INCOMES","EXPENSES","FINANCING_DEBTS","FINANCING_CREDITOR","CREDITS"),
                          classificationType, # "PROGRAM","FUNCTIONAL","ECONOMIC","CREDIT"
                          year,
                          period = "MONTH") {

  api_base <- "https://api.openbudget.gov.ua/api/public/localBudgetData?"

  if (budgetItem %in% c("EXPENSES", "CREDITS")) {
    api_path <-
      paste(api_base,
            "budgetCode=", budgetCode,
            "&budgetItem=", budgetItem,
            "&classificationType=", classificationType,  # classificationType parameter is mandatory for EXPENSES and CREDITS items
            "&period=", period,
            "&year=", year,
            sep = "")
  } else {
    api_path <-
      paste(api_base,
            "budgetCode=", budgetCode,
            "&budgetItem=", budgetItem,
            "&period=", period,
            "&year=", year,
            sep = "")
  }

  return(api_path)
}

# Function to call API, read in and parse data
call_api <- function(api_path, col_types) {
  data_call <- GET(api_path) |>
    pluck("content") |>
    rawToChar() |>
    read_delim(delim = ";", col_types = col_types) |>
    mutate(REP_PERIOD = readr::parse_date(REP_PERIOD, "%m.%Y") |>
             ceiling_date(unit = "month") - days(1))  # Use end of month dates

  return(data_call)
}

# Function to download data
download_data <- function(BUDGETCODE, YEAR) {
  var_types <- read_csv("inputs/variable_types.csv")

  # Construct API calls
  df_api <- var_types |>
    group_by(budgetItem, classificationType) |>
    summarise(col_type = paste(colType, collapse = ""), .groups = "drop") |>
    expand_grid(budgetCode = BUDGETCODE, year = YEAR) |>
    rowwise() |>
    mutate(api_path = api_construct(budgetCode, budgetItem, classificationType, year))

  # Read in data across multiple periods and categories into a nested data frame
  df_n <- df_api |>
    mutate(data = list(call_api(api_path, col_type))) |>
    select(budgetItem, classificationType, data) |>
    group_by(budgetItem, classificationType) |>
    summarise(data = list(list_rbind(data) |> arrange(REP_PERIOD)))


  # Extract nested data column into a named list
  data_l <- df_n$data
  names(data_l) <- if_else(!is.na(df_n$classificationType),
                           paste(df_n$budgetItem, df_n$classificationType, sep=", "),
                           df_n$budgetItem)

  return(data_l)
}

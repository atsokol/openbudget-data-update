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

# Function to call API, read in and parse data with retry logic
call_api <- function(api_path, col_types, max_retries = 3) {
  last_error <- ""

  for (attempt in seq_len(max_retries)) {
    # Pause before retries: longer after rate-limit (429), shorter otherwise
    if (attempt > 1) {
      wait_secs <- if (grepl("rate_limited_429", last_error)) 10 else 3
      message(sprintf("  Retrying in %d seconds (attempt %d/%d)...",
                      wait_secs, attempt, max_retries))
      Sys.sleep(wait_secs)
    }

    result <- tryCatch({
      response <- GET(api_path, timeout(30))
      status   <- status_code(response)

      if (status == 429) stop("rate_limited_429")
      if (status != 200) stop(sprintf("HTTP %d", status))
      if (length(response$content) == 0) stop("empty_response_body")

      data_call <- response |>
        pluck("content") |>
        rawToChar() |>
        read_delim(delim = ";", col_types = col_types, show_col_types = FALSE) |>
        mutate(REP_PERIOD = readr::parse_date(REP_PERIOD, "%m.%Y") |>
                 ceiling_date(unit = "month") - days(1))

      if (nrow(data_call) == 0) {
        message(sprintf("  No data returned from API (attempt %d)", attempt))
      }

      Sys.sleep(0.2)  # polite delay between successful calls
      data_call

    }, error = function(e) {
      last_error <<- e$message
      message(sprintf("  API call failed (attempt %d/%d): %s",
                      attempt, max_retries, e$message))
      NULL
    })

    if (!is.null(result)) return(result)
  }

  stop(sprintf("Failed to call API after %d attempts.\nURL: %s\nLast error: %s",
               max_retries, api_path, last_error))
}

# Function to download data
download_data <- function(BUDGETCODE, YEAR) {
  if (!file.exists("inputs/variable_types.csv")) {
    stop("Variable types file not found: inputs/variable_types.csv")
  }
  
  var_types <- read_csv("inputs/variable_types.csv", show_col_types = FALSE)
  
  if (nrow(var_types) == 0) {
    stop("variable_types.csv is empty")
  }

  # Construct API calls
  df_api <- var_types |>
    group_by(budgetItem, classificationType) |>
    summarise(col_type = paste(colType, collapse = ""), .groups = "drop") |>
    expand_grid(budgetCode = BUDGETCODE, year = YEAR) |>
    rowwise() |>
    mutate(api_path = api_construct(budgetCode, budgetItem, classificationType, year))

  message(sprintf("Downloading data for %d budget codes, %d years, %d API calls", 
                 length(unique(BUDGETCODE)), length(unique(YEAR)), nrow(df_api)))

  # Read in data across multiple periods and categories into a nested data frame
  # Process sequentially to avoid overwhelming the API
  df_n <- df_api |>
    rowwise() |>
    mutate(data = list(call_api(api_path, col_type))) |>
    ungroup() |>
    select(budgetItem, classificationType, data) |>
    group_by(budgetItem, classificationType) |>
    summarise(data = list(bind_rows(data) |> arrange(REP_PERIOD)), .groups = "drop")

  # Validate data was retrieved
  if (nrow(df_n) == 0) {
    stop("No data retrieved from API")
  }

  # Extract nested data column into a named list
  data_l <- df_n$data
  names(data_l) <- if_else(!is.na(df_n$classificationType),
                           paste(df_n$budgetItem, df_n$classificationType, sep=", "),
                           df_n$budgetItem)

  return(data_l)
}

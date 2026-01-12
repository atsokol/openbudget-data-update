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
  for (attempt in seq_len(max_retries)) {
    result <- tryCatch({
      # Add small delay to avoid rate limiting
      if (attempt > 1) {
        wait_time <- min(2^(attempt-1), 5)
        Sys.sleep(wait_time)
      }
      
      response <- GET(api_path, timeout(30))
      
      # Check HTTP status
      if (status_code(response) != 200) {
        if (status_code(response) == 429) {
          # Rate limited, retry with longer wait
          if (attempt < max_retries) {
            message(sprintf("Rate limited (429), retrying in %d seconds...", 10))
            Sys.sleep(10)
            return(NULL)
          }
        }
        stop(sprintf("API request failed with status %d", status_code(response)))
      }
      
      # Check if response has content
      if (length(response$content) == 0) {
        warning(sprintf("Empty response from API (attempt %d)", attempt))
        return(tibble())
      }
      
      # Parse response
      data_call <- response |>
        pluck("content") |>
        rawToChar() |>
        read_delim(delim = ";", col_types = col_types, show_col_types = FALSE) |>
        mutate(REP_PERIOD = readr::parse_date(REP_PERIOD, "%m.%Y") |>
                 ceiling_date(unit = "month") - days(1))  # Use end of month dates
      
      # Validate parsed data
      if (nrow(data_call) == 0) {
        warning(sprintf("No data returned from API"))
      }
      
      # Add small delay between successful calls
      Sys.sleep(0.2)
      
      return(data_call)
    }, error = function(e) {
      if (attempt < max_retries) {
        message(sprintf("API call failed (attempt %d/%d): %s", attempt, max_retries, e$message))
        return(NULL)
      } else {
        stop(sprintf("Failed to call API after %d attempts: %s\nURL: %s", 
                    max_retries, e$message, api_path))
      }
    })
    
    if (!is.null(result)) {
      return(result)
    }
  }
  
  # Should not reach here, but return empty tibble as fallback
  return(tibble())
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

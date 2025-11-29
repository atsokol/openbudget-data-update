clean_csv_folder <- function(folder) {
  # Ensure folder path ends with /
  folder <- normalizePath(folder, mustWork = TRUE)

  # List all CSV files
  files <- list.files(folder, pattern = "\\.csv$", full.names = TRUE)

  # If no files, exit
  if (length(files) == 0) {
    message("No CSV files found in folder.")
    return(invisible(NULL))
  }

  # Process each file
  for (file in files) {

    message("Cleaning: ", basename(file))

    # Load the CSV
    df <- read.csv(file, stringsAsFactors = FALSE)

    # Remove complete duplicate rows
    df_clean <- unique(df)
    # or dplyr: df_clean <- dplyr::distinct(df)

    # Write cleaned file back (overwrite)
    write.csv(df_clean, file, row.names = FALSE)
  }

  message("Cleaning complete.")
}

clean_csv_folder("data")

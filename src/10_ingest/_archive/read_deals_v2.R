# =============================================================================
# src/10_ingest/read_deals_v2.R
# =============================================================================
# Flexible, config-driven M&A deal data ingestion
#
# Features:
# - Schema-based column mapping (handles multiple source formats)
# - Comprehensive data validation with quality reporting
# - Derived variable creation with traceability
# - Detailed logging and diagnostic output
#
# Usage:
#   Rscript src/10_ingest/read_deals_v2.R [--schema config/data_schema.yaml]
#
# Output:
#   - data/interim/deals_clean.csv (cleaned data)
#   - data/interim/deals_quality_report.html (validation results)
#   - data/interim/deals_ingestion_log.txt (processing log)
# =============================================================================

suppressPackageStartupMessages({
  library(readxl)
  library(yaml)
  library(dplyr)
  library(stringr)
  library(lubridate)
  library(readr)
  library(fs)
  library(glue)
  library(purrr)
  library(tidyr)
})

# =============================================================================
# CONFIGURATION
# =============================================================================

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)
schema_path <- if (length(args) > 0) args[1] else "config/data_schema.yaml"

if (!file.exists(schema_path)) {
  stop(glue("Schema file not found: {schema_path}"))
}

# Load schema
schema <- yaml::read_yaml(schema_path)

# Set up logging
log_path <- "data/interim/deals_ingestion_log.txt"
fs::dir_create(dirname(log_path))
log_conn <- file(log_path, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  message(full_msg)
}

log_msg("=== M&A Deal Data Ingestion ===", "INFO")
log_msg(glue("Schema: {schema_path}"), "INFO")

# =============================================================================
# DATA LOADING
# =============================================================================

log_msg("Loading raw data...", "INFO")

# Find first .xlsx file in data/raw
raw_files <- fs::dir_ls("data/raw", regexp = "\\.xlsx$")
if (length(raw_files) == 0) {
  log_msg("No .xlsx files found in data/raw", "ERROR")
  stop("No input data found")
}

raw_path <- raw_files[1]
log_msg(glue("Input file: {basename(raw_path)}"), "INFO")

# Read raw data
df_raw <- tryCatch({
  readxl::read_excel(raw_path, sheet = 1)
}, error = function(e) {
  log_msg(glue("Failed to read Excel file: {e$message}"), "ERROR")
  stop(e)
})

log_msg(glue("Raw dimensions: {nrow(df_raw)} rows × {ncol(df_raw)} columns"), "INFO")

# Clean column names (lowercase, underscores, remove special chars)
df_raw <- df_raw %>%
  janitor::clean_names()

# Remove completely empty rows
df_raw <- df_raw %>%
  filter(if_any(everything(), ~ !is.na(.)))

log_msg(glue("After removing empty rows: {nrow(df_raw)} rows"), "INFO")

# Remove unnamed columns
df_raw <- df_raw %>%
  select(!starts_with("unnamed"))

log_msg(glue("Raw columns: {paste(names(df_raw), collapse = ', ')}"), "DEBUG")

# =============================================================================
# COLUMN MAPPING
# =============================================================================

log_msg("Mapping columns to standard schema...", "INFO")

# Build mapping dictionary: standard_name -> list of possible source names
col_map <- schema$columns %>%
  map(~ .x$source_names) %>%
  set_names(names(schema$columns))

# Function to find source column for a standard name
find_source_col <- function(standard_name, df_names) {
  possible_names <- col_map[[standard_name]]
  matches <- intersect(possible_names, df_names)
  
  if (length(matches) == 0) {
    return(NA_character_)
  } else if (length(matches) > 1) {
    log_msg(glue("Multiple matches for '{standard_name}': {paste(matches, collapse = ', ')}. Using first."), "WARN")
    return(matches[1])
  } else {
    return(matches[1])
  }
}

# Create mapping table
mapping_table <- tibble(
  standard_name = names(col_map),
  source_name = map_chr(standard_name, ~ find_source_col(.x, names(df_raw))),
  required = map_lgl(standard_name, ~ schema$columns[[.x]]$required %||% FALSE),
  found = !is.na(source_name)
)

# Log mapping results
log_msg("Column mapping results:", "INFO")
for (i in seq_len(nrow(mapping_table))) {
  row <- mapping_table[i, ]
  if (row$found) {
    log_msg(glue("  ✓ {row$standard_name} <- {row$source_name}"), "INFO")
  } else if (row$required) {
    log_msg(glue("  ✗ {row$standard_name} [REQUIRED] <- NOT FOUND"), "ERROR")
  } else {
    log_msg(glue("  - {row$standard_name} [optional] <- not found"), "DEBUG")
  }
}

# Check required columns
missing_required <- mapping_table %>%
  filter(required, !found) %>%
  pull(standard_name)

if (length(missing_required) > 0) {
  err_msg <- glue("Missing required columns: {paste(missing_required, collapse = ', ')}")
  log_msg(err_msg, "ERROR")
  
  if (schema$processing$missing_required_action == "error") {
    stop(err_msg)
  } else if (schema$processing$missing_required_action == "warn") {
    warning(err_msg)
  }
}

# Rename columns
df_mapped <- df_raw

for (i in seq_len(nrow(mapping_table))) {
  if (mapping_table$found[i]) {
    std_name <- mapping_table$standard_name[i]
    src_name <- mapping_table$source_name[i]
    df_mapped <- df_mapped %>%
      rename(!!std_name := !!src_name)
  }
}

# Keep only mapped columns (drop unmapped source columns if configured)
if (!schema$output$include_source_columns) {
  mapped_cols <- mapping_table %>%
    filter(found) %>%
    pull(standard_name)
  
  df_mapped <- df_mapped %>%
    select(all_of(mapped_cols))
  
  log_msg(glue("Keeping {length(mapped_cols)} mapped columns"), "INFO")
}

# =============================================================================
# DATA TYPE CONVERSION
# =============================================================================

log_msg("Converting data types...", "INFO")

# Helper: clean numeric strings (remove commas, asterisks)
clean_numeric <- function(x) {
  x %>%
    as.character() %>%
    str_replace_all(",", "") %>%
    str_replace_all("\\*", "") %>%
    str_replace_all("\\s+", "") %>%
    as.numeric()
}

# Helper: parse dates flexibly
parse_date_flexible <- function(x, formats = schema$processing$date_formats) {
  if (is.Date(x) || lubridate::is.POSIXct(x)) {
    return(as.Date(x))
  }
  
  for (fmt in formats) {
    result <- suppressWarnings(as.Date(x, format = fmt))
    if (sum(!is.na(result)) > 0.5 * length(x)) {  # >50% success rate
      return(result)
    }
  }
  
  # Fallback: let lubridate try
  return(lubridate::as_date(x))
}

# Apply conversions based on schema
for (col_name in names(schema$columns)) {
  if (col_name %in% names(df_mapped)) {
    col_spec <- schema$columns[[col_name]]
    col_type <- col_spec$type
    
    df_mapped <- df_mapped %>%
      mutate(!!sym(col_name) := case_when(
        col_type == "numeric" ~ clean_numeric(!!sym(col_name)),
        col_type == "integer" ~ as.integer(clean_numeric(!!sym(col_name))),
        col_type == "date" ~ parse_date_flexible(!!sym(col_name)),
        col_type == "character" ~ as.character(!!sym(col_name)),
        TRUE ~ !!sym(col_name)
      ))
    
    log_msg(glue("  Converted {col_name} to {col_type}"), "DEBUG")
  }
}

# String cleaning (if configured)
if (schema$processing$trim_whitespace) {
  char_cols <- df_mapped %>%
    select(where(is.character)) %>%
    names()
  
  df_mapped <- df_mapped %>%
    mutate(across(all_of(char_cols), str_trim))
}

# Ticker uppercase (if configured)
if (schema$processing$uppercase_tickers && "target_ticker" %in% names(df_mapped)) {
  df_mapped <- df_mapped %>%
    mutate(target_ticker = toupper(target_ticker))
}

log_msg("Data type conversion complete", "INFO")

# =============================================================================
# DERIVED VARIABLES
# =============================================================================

log_msg("Creating derived variables...", "INFO")

for (var_name in names(schema$derived_variables)) {
  var_spec <- schema$derived_variables[[var_name]]
  formula_str <- var_spec$formula
  
  log_msg(glue("  Creating {var_name}: {formula_str}"), "DEBUG")
  
  # Evaluate formula in context of dataframe
  # Note: This uses rlang::parse_expr for safety
  tryCatch({
    df_mapped <- df_mapped %>%
      mutate(!!sym(var_name) := eval(rlang::parse_expr(formula_str)))
    
    log_msg(glue("  ✓ {var_name} created"), "INFO")
  }, error = function(e) {
    log_msg(glue("  ✗ Failed to create {var_name}: {e$message}"), "ERROR")
    warning(glue("Derived variable {var_name} creation failed"))
  })
}

# =============================================================================
# DATA QUALITY VALIDATION
# =============================================================================

log_msg("Running data quality checks...", "INFO")

quality_issues <- list()

for (check_name in names(schema$quality_checks)) {
  check_spec <- schema$quality_checks[[check_name]]
  log_msg(glue("  {check_name}: {check_spec$description}"), "INFO")
  
  for (rule in check_spec$rules) {
    check_expr <- rule$check
    severity <- rule$severity
    message_text <- rule$message
    
    # Evaluate check expression
    tryCatch({
      check_result <- df_mapped %>%
        mutate(check_pass = eval(rlang::parse_expr(check_expr))) %>%
        pull(check_pass)
      
      n_fail <- sum(!check_result, na.rm = TRUE)
      n_total <- length(check_result)
      pct_fail <- round(100 * n_fail / n_total, 2)
      
      if (n_fail > 0) {
        issue_msg <- glue("{message_text}: {n_fail}/{n_total} rows ({pct_fail}%)")
        
        quality_issues[[length(quality_issues) + 1]] <- list(
          check_name = check_name,
          rule = check_expr,
          severity = severity,
          message = issue_msg,
          n_fail = n_fail,
          pct_fail = pct_fail
        )
        
        log_msg(glue("    [{severity}] {issue_msg}"), severity)
        
        if (severity == "error") {
          warning(issue_msg)
        }
      } else {
        log_msg(glue("    ✓ {message_text}: all pass"), "INFO")
      }
      
    }, error = function(e) {
      log_msg(glue("    ✗ Check failed to evaluate: {e$message}"), "ERROR")
    })
  }
}

# =============================================================================
# FILTERING
# =============================================================================

log_msg("Applying filters...", "INFO")

df_clean <- df_mapped %>%
  filter(!is.na(target_ticker) | !is.na(target_name))

n_filtered <- nrow(df_mapped) - nrow(df_clean)
log_msg(glue("Filtered out {n_filtered} rows with missing target identifier"), "INFO")
log_msg(glue("Final dataset: {nrow(df_clean)} rows × {ncol(df_clean)} columns"), "INFO")

# =============================================================================
# SAVE OUTPUT
# =============================================================================

log_msg("Saving output...", "INFO")

# Ensure output directory exists
fs::dir_create(dirname(schema$output$file))

# Save clean data
readr::write_csv(
  df_clean, 
  schema$output$file,
  na = schema$output$na_string
)

log_msg(glue("✓ Saved clean data: {schema$output$file}"), "INFO")

# Save quality issues report
if (length(quality_issues) > 0) {
  quality_df <- bind_rows(quality_issues)
  readr::write_csv(quality_df, "data/interim/deals_quality_issues.csv")
  log_msg("✓ Saved quality issues: data/interim/deals_quality_issues.csv", "INFO")
}

# Save column mapping for documentation
readr::write_csv(mapping_table, "data/interim/deals_column_mapping.csv")
log_msg("✓ Saved column mapping: data/interim/deals_column_mapping.csv", "INFO")

# Summary statistics
summary_stats <- list(
  input_file = basename(raw_path),
  raw_rows = nrow(df_raw),
  raw_cols = ncol(df_raw),
  clean_rows = nrow(df_clean),
  clean_cols = ncol(df_clean),
  cols_mapped = sum(mapping_table$found),
  cols_missing = sum(!mapping_table$found),
  required_missing = length(missing_required),
  quality_issues = length(quality_issues),
  timestamp = Sys.time()
)

jsonlite::write_json(
  summary_stats, 
  "data/interim/deals_ingestion_summary.json",
  pretty = TRUE,
  auto_unbox = TRUE
)

log_msg("✓ Saved summary: data/interim/deals_ingestion_summary.json", "INFO")

# =============================================================================
# GENERATE DATA QUALITY REPORT
# =============================================================================

log_msg("Generating data quality report...", "INFO")

# Use skimr or DataExplorer for comprehensive report
if (require("DataExplorer", quietly = TRUE)) {
  DataExplorer::create_report(
    df_clean,
    output_file = "deals_quality_report.html",
    output_dir = "data/interim",
    y = "deal_completed",
    report_title = "M&A Deals Data Quality Report"
  )
  log_msg("✓ Generated report: data/interim/deals_quality_report.html", "INFO")
} else {
  log_msg("DataExplorer not available - skipping HTML report", "WARN")
}

# =============================================================================
# COMPLETION
# =============================================================================

log_msg("=== Ingestion Complete ===", "INFO")
log_msg(glue("Final output: {nrow(df_clean)} deals ready for analysis"), "INFO")

close(log_conn)

# Print summary to console
cat("\n")
cat("=" %>% rep(70) %>% paste(collapse = ""), "\n")
cat("M&A DEAL DATA INGESTION SUMMARY\n")
cat("=" %>% rep(70) %>% paste(collapse = ""), "\n")
cat(glue("Input:  {basename(raw_path)}"), "\n")
cat(glue("Output: {schema$output$file}"), "\n")
cat(glue("Rows:   {nrow(df_raw)} → {nrow(df_clean)} (filtered {n_filtered})"), "\n")
cat(glue("Cols:   {ncol(df_raw)} → {ncol(df_clean)} (mapped {sum(mapping_table$found)})"), "\n")
if (length(quality_issues) > 0) {
  cat(glue("Issues: {length(quality_issues)} quality checks failed (see log)"), "\n")
}
cat("=" %>% rep(70) %>% paste(collapse = ""), "\n")

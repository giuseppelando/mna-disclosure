# =============================================================================
# src/10_ingest/read_deals_v2_refined.R
# =============================================================================
# Research-design aligned M&A deal data ingestion
#
# Refinements over v2:
# - Explicit deduplication with logging
# - Outcome variables aligned with research design (DL-02, DL-03)
# - Sample filter flags instead of immediate dropping
# - Proper SIC parsing and industry classification
# - Payment method standardization
# - Comprehensive summary statistics for thesis documentation
#
# Usage:
#   Rscript src/10_ingest/read_deals_v2_refined.R
#
# Output:
#   - data/interim/deals_ingested.rds (all rows, cleaned and derived variables)
#   - data/interim/deals_ingestion_log.txt (detailed processing log)
#   - data/interim/deals_ingestion_summary.json (statistics for documentation)
#   - data/interim/deals_column_mapping.csv (schema mapping record)
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
  library(janitor)
})

# =============================================================================
# SETUP
# =============================================================================

# Load schema
schema_path <- "config/data_schema.yaml"
if (!file.exists(schema_path)) {
  stop(glue("Schema file not found: {schema_path}"))
}
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

# Sample size tracking helper
track_sample <- function(df, step_name) {
  n <- nrow(df)
  log_msg(glue("After {step_name}: {n} rows"), "INFO")
  return(n)
}

log_msg("=================================================================", "INFO")
log_msg("M&A DEAL DATA INGESTION (Research Design Aligned)", "INFO")
log_msg("=================================================================", "INFO")
log_msg(glue("Schema: {schema_path}"), "INFO")
log_msg(glue("Start time: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"), "INFO")

# =============================================================================
# STEP 1: DATA LOADING
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 1: Loading raw data", "INFO")
log_msg("-----------------------------------------------------------------", "INFO")

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

n_raw <- track_sample(df_raw, "initial load")
log_msg(glue("Raw dimensions: {n_raw} rows × {ncol(df_raw)} columns"), "INFO")

# Clean column names (lowercase, underscores, remove special chars)
df_raw <- df_raw %>%
  janitor::clean_names()

log_msg(glue("Raw column names (first 10): {paste(head(names(df_raw), 10), collapse = ', ')}"), "DEBUG")

# =============================================================================
# STEP 2: REMOVE EMPTY ROWS AND UNNAMED COLUMNS
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 2: Removing empty rows and unnamed columns", "INFO")
log_msg("-----------------------------------------------------------------", "INFO")

# Remove completely empty rows (all fields NA)
n_before <- nrow(df_raw)
df_raw <- df_raw %>%
  filter(if_any(everything(), ~ !is.na(.)))
n_after <- nrow(df_raw)
n_empty <- n_before - n_after
log_msg(glue("Removed {n_empty} completely empty rows"), "INFO")

# Remove unnamed columns (artifacts from Excel export)
unnamed_cols <- names(df_raw)[str_detect(names(df_raw), "^unnamed")]
if (length(unnamed_cols) > 0) {
  log_msg(glue("Removing {length(unnamed_cols)} unnamed columns"), "INFO")
  df_raw <- df_raw %>%
    select(!starts_with("unnamed"))
}

track_sample(df_raw, "empty row/column removal")

# =============================================================================
# STEP 3: DEDUPLICATION
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 3: Deduplication", "INFO")
log_msg("-----------------------------------------------------------------", "INFO")

# Check for duplicates before any transformations
n_before <- nrow(df_raw)
n_dupes <- sum(duplicated(df_raw))
log_msg(glue("Found {n_dupes} duplicate rows ({round(100*n_dupes/n_before, 1)}% of data)"), "WARN")

if (n_dupes > 0) {
  # Deduplication strategy: keep first occurrence based on deal_number if available
  # Rationale: Deal Number is the primary key in SDC/Refinitiv data
  
  deal_num_col <- intersect(
    c("deal_number", "deal_id", "dealid", "transaction_id"),
    names(df_raw)
  )[1]
  
  if (!is.na(deal_num_col)) {
    log_msg(glue("Deduplicating by {deal_num_col} (keeping first occurrence)"), "INFO")
    
    # Group by deal number, keep first row per group, then add exact duplicates
    df_deduped <- df_raw %>%
      filter(!is.na(!!sym(deal_num_col))) %>%
      distinct(!!sym(deal_num_col), .keep_all = TRUE)
    
    # For rows with missing deal_number, remove exact duplicates only
    df_missing_id <- df_raw %>%
      filter(is.na(!!sym(deal_num_col))) %>%
      distinct()
    
    df_raw <- bind_rows(df_deduped, df_missing_id)
    
    n_after <- nrow(df_raw)
    n_removed <- n_before - n_after
    log_msg(glue("Removed {n_removed} duplicate rows"), "INFO")
    log_msg(glue("Deduplication rule: First occurrence by {deal_num_col}, exact match for missing ID"), "INFO")
  } else {
    log_msg("No deal ID column found - removing only exact duplicate rows", "WARN")
    df_raw <- df_raw %>% distinct()
    n_after <- nrow(df_raw)
    n_removed <- n_before - n_after
    log_msg(glue("Removed {n_removed} exact duplicate rows"), "INFO")
  }
}

track_sample(df_raw, "deduplication")

# =============================================================================
# STEP 4: COLUMN MAPPING
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 4: Mapping columns to standard schema", "INFO")
log_msg("-----------------------------------------------------------------", "INFO")

# Build mapping dictionary
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
    log_msg(glue("  Multiple matches for '{standard_name}': {paste(matches, collapse = ', ')}. Using first."), "WARN")
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

# Log mapping results (concise)
n_mapped <- sum(mapping_table$found)
n_required_missing <- sum(mapping_table$required & !mapping_table$found)
log_msg(glue("Mapped {n_mapped}/{nrow(mapping_table)} columns"), "INFO")

if (n_required_missing > 0) {
  missing_req <- mapping_table %>%
    filter(required, !found) %>%
    pull(standard_name)
  log_msg(glue("  Missing required columns: {paste(missing_req, collapse = ', ')}"), "ERROR")
}

# Log detailed mapping to DEBUG level
log_msg("Column mapping details:", "DEBUG")
for (i in seq_len(nrow(mapping_table))) {
  row <- mapping_table[i, ]
  if (row$found) {
    log_msg(glue("  ✓ {row$standard_name} <- {row$source_name}"), "DEBUG")
  } else if (row$required) {
    log_msg(glue("  ✗ {row$standard_name} [REQUIRED] <- NOT FOUND"), "ERROR")
  } else {
    log_msg(glue("  - {row$standard_name} [optional] <- not found"), "DEBUG")
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

# Keep only mapped columns (cleaner downstream processing)
mapped_cols <- mapping_table %>%
  filter(found) %>%
  pull(standard_name)

df_mapped <- df_mapped %>%
  select(all_of(mapped_cols))

log_msg(glue("Retained {length(mapped_cols)} mapped columns"), "INFO")
track_sample(df_mapped, "column mapping")

# =============================================================================
# STEP 5: DATA TYPE CONVERSION
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 5: Converting data types", "INFO")
log_msg("-----------------------------------------------------------------", "INFO")

# Helper: clean numeric strings (remove commas, asterisks, whitespace)
clean_numeric <- function(x) {
  x %>%
    as.character() %>%
    str_replace_all(",", "") %>%
    str_replace_all("\\*", "") %>%
    str_replace_all("\\s+", "") %>%
    str_replace_all("n\\.a\\.", NA_character_) %>%
    as.numeric()
}

# Helper: parse dates flexibly
parse_date_flexible <- function(x) {
  # If already Date/POSIXct, convert and return
  if (inherits(x, "Date")) {
    return(as.Date(x))
  }
  if (inherits(x, "POSIXct")) {
    return(as.Date(x))
  }
  
  # Try lubridate auto-parse (handles most formats)
  result <- suppressWarnings(lubridate::as_date(x))
  
  # Check success rate (>50% is good enough)
  if (sum(!is.na(result)) > 0.5 * length(x)) {
    return(result)
  }
  
  # Fallback to explicit formats from schema
  for (fmt in schema$processing$date_formats) {
    result <- suppressWarnings(as.Date(as.character(x), format = fmt))
    if (sum(!is.na(result)) > 0.5 * length(x)) {
      return(result)
    }
  }
  
  # If all fails, return original with warning
  log_msg(glue("Date parsing struggled for column (high NA rate)"), "WARN")
  return(result)
}

# Apply conversions based on schema
conversion_log <- list()

for (col_name in names(schema$columns)) {
  if (col_name %in% names(df_mapped)) {
    col_spec <- schema$columns[[col_name]]
    col_type <- col_spec$type
    
    # Track NAs before/after conversion for diagnostics
    na_before <- sum(is.na(df_mapped[[col_name]]))
    
    # Apply conversion based on target type
    # Each type gets its own direct conversion (no case_when to avoid type conflicts)
    if (col_type == "numeric") {
      df_mapped[[col_name]] <- clean_numeric(df_mapped[[col_name]])
    } else if (col_type == "integer") {
      df_mapped[[col_name]] <- as.integer(clean_numeric(df_mapped[[col_name]]))
    } else if (col_type == "date") {
      df_mapped[[col_name]] <- parse_date_flexible(df_mapped[[col_name]])
    } else if (col_type == "character") {
      df_mapped[[col_name]] <- as.character(df_mapped[[col_name]])
    }
    # If type not recognized, leave column as-is (no else clause needed)
    
    na_after <- sum(is.na(df_mapped[[col_name]]))
    
    conversion_log[[col_name]] <- list(
      target_type = col_type,
      na_before = na_before,
      na_after = na_after,
      na_introduced = na_after - na_before
    )
    
    if (na_after > na_before) {
      log_msg(glue("  {col_name} ({col_type}): +{na_after - na_before} NAs introduced in conversion"), "WARN")
    }
  }
}

# String cleaning (trim whitespace)
if (schema$processing$trim_whitespace) {
  char_cols <- df_mapped %>%
    select(where(is.character)) %>%
    names()
  
  df_mapped <- df_mapped %>%
    mutate(across(all_of(char_cols), str_trim))
}

# Ticker uppercase
if (schema$processing$uppercase_tickers && "target_ticker" %in% names(df_mapped)) {
  df_mapped <- df_mapped %>%
    mutate(target_ticker = toupper(target_ticker))
  log_msg("  Standardized target_ticker to uppercase", "DEBUG")
}

log_msg("Data type conversion complete", "INFO")
track_sample(df_mapped, "type conversion")

# =============================================================================
# STEP 6: DERIVED VARIABLES (RESEARCH DESIGN ALIGNED)
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 6: Creating derived variables (research design aligned)", "INFO")
log_msg("-----------------------------------------------------------------", "INFO")

# 6A: Deal Outcome Variables (DL-02, DL-03 compliance)
log_msg("Creating outcome variables...", "INFO")

df_mapped <- df_mapped %>%
  mutate(
    # Clean deal status string
    deal_status_clean = str_to_lower(str_trim(deal_status)),
    
    # Terminal outcome classification (per DL-03)
    deal_outcome_terminal = case_when(
      deal_status_clean == "completed" ~ "completed",
      deal_status_clean == "withdrawn" ~ "withdrawn",
      deal_status_clean == "completed assumed" ~ "completed_assumed",
      TRUE ~ "other"
    ),
    
    # Binary completion indicator (per research design)
    # Note: "Completed Assumed" treated as pseudo-completion, excluded from main analysis
    deal_completed = case_when(
      deal_outcome_terminal == "completed" ~ 1L,
      deal_outcome_terminal == "withdrawn" ~ 0L,
      TRUE ~ NA_integer_
    ),
    
    # Time to close (days from announcement to terminal outcome)
    # For withdrawn deals, this represents censoring time for survival models
    time_to_close = as.integer(
      coalesce(complete_date, withdrawn_date) - announce_date
    )
  )

# Log outcome distribution
outcome_dist <- df_mapped %>%
  count(deal_outcome_terminal) %>%
  arrange(desc(n))

log_msg("Deal outcome distribution:", "INFO")
for (i in seq_len(nrow(outcome_dist))) {
  row <- outcome_dist[i, ]
  pct <- round(100 * row$n / sum(outcome_dist$n), 1)
  log_msg(glue("  {row$deal_outcome_terminal}: {row$n} ({pct}%)"), "INFO")
}

# 6B: Industry Classification (SIC parsing)
log_msg("Creating industry classification variables...", "INFO")

df_mapped <- df_mapped %>%
  mutate(
    # Extract first SIC code (most deals have multiple separated by " / ")
    # Rule: Use first code as it's typically the primary business line
    target_sic_primary = str_extract(target_sic, "^\\d{4}"),
    
    # 2-digit SIC for broader industry grouping
    target_sic_2digit = str_sub(target_sic_primary, 1, 2),
    
    # Industry category (simple Fama-French-style mapping)
    # Can be refined later with full FF classification
    industry_broad = case_when(
      target_sic_2digit >= "01" & target_sic_2digit <= "09" ~ "Agriculture",
      target_sic_2digit >= "10" & target_sic_2digit <= "14" ~ "Mining",
      target_sic_2digit >= "15" & target_sic_2digit <= "17" ~ "Construction",
      target_sic_2digit >= "20" & target_sic_2digit <= "39" ~ "Manufacturing",
      target_sic_2digit >= "40" & target_sic_2digit <= "49" ~ "Transportation",
      target_sic_2digit >= "50" & target_sic_2digit <= "59" ~ "Wholesale_Retail",
      target_sic_2digit >= "60" & target_sic_2digit <= "67" ~ "Finance",
      target_sic_2digit >= "70" & target_sic_2digit <= "89" ~ "Services",
      target_sic_2digit >= "91" & target_sic_2digit <= "99" ~ "PublicAdmin",
      TRUE ~ "Other"
    )
  )

n_sic_parsed <- sum(!is.na(df_mapped$target_sic_primary))
log_msg(glue("  Parsed SIC codes: {n_sic_parsed}/{nrow(df_mapped)} rows"), "INFO")

# 6C: Payment Method Standardization
log_msg("Standardizing payment method categories...", "INFO")

df_mapped <- df_mapped %>%
  mutate(
    payment_method_clean = case_when(
      # Standardize to canonical categories for econometric controls
      str_detect(str_to_lower(payment_method), "cash") ~ "Cash",
      str_detect(str_to_lower(payment_method), "share|stock|equity") ~ "Stock",
      str_detect(str_to_lower(payment_method), "mixed|combination|cash.*share|share.*cash") ~ "Mixed",
      str_detect(str_to_lower(payment_method), "liabilit") ~ "Mixed",  # Liabilities often mixed
      !is.na(payment_method) ~ "Other",
      TRUE ~ NA_character_
    )
  )

# Log payment method distribution
payment_dist <- df_mapped %>%
  count(payment_method_clean) %>%
  arrange(desc(n))

log_msg("Payment method distribution:", "INFO")
for (i in seq_len(nrow(payment_dist))) {
  row <- payment_dist[i, ]
  pct <- round(100 * row$n / sum(payment_dist$n, na.rm = TRUE), 1)
  log_msg(glue("  {row$payment_method_clean}: {row$n} ({pct}%)"), "INFO")
}

# 6D: Fixed Effects Identifiers
log_msg("Creating fixed effects identifiers...", "INFO")

df_mapped <- df_mapped %>%
  mutate(
    # Year of announcement (for year FE)
    year_announced = lubridate::year(announce_date),
    
    # Industry × Year identifier (for granular FE)
    industry_year = paste0(industry_broad, "_", year_announced)
  )

# 6E: Sample Filter Flags (NOT applied yet, just flagged for transparency)
log_msg("Creating sample filter flags...", "INFO")

df_mapped <- df_mapped %>%
  mutate(
    # DL-03: Has terminal outcome (Completed or Withdrawn)
    flag_terminal_outcome = deal_outcome_terminal %in% c("completed", "withdrawn"),
    
    # DL-04: Has non-missing announcement date (temporal anchor)
    flag_has_announce_date = !is.na(announce_date),
    
    # DL-09: Control transfer criterion (stake >= 50% or deal type indicates acquisition)
    flag_control_transfer = (
      final_stake_pct >= 50 | 
      str_detect(str_to_lower(deal_type), "acquisition|merger")
    ),
    
    # Premium analysis readiness: Has offer price
    flag_has_offer_price = !is.na(offer_price_eur),
    
    # Has identifiable target (ticker or name)
    flag_has_target_id = !is.na(target_ticker) | !is.na(target_name),
    
    # US listing indicator (inferred from exchange string)
    # Note: May need refinement based on actual SDC data format
    flag_us_listed = case_when(
      is.na(target_exchange) ~ NA,
      str_detect(str_to_lower(target_exchange), "nyse|nasdaq|amex") ~ TRUE,
      TRUE ~ FALSE
    )
  )

# Log filter flag summary
flag_summary <- df_mapped %>%
  summarise(
    across(starts_with("flag_"), ~ sum(.x, na.rm = TRUE))
  ) %>%
  pivot_longer(everything(), names_to = "flag", values_to = "n") %>%
  mutate(
    pct = round(100 * n / nrow(df_mapped), 1)
  )

log_msg("Sample filter flag summary (rows passing each criterion):", "INFO")
for (i in seq_len(nrow(flag_summary))) {
  row <- flag_summary[i, ]
  log_msg(glue("  {row$flag}: {row$n} ({row$pct}%)"), "INFO")
}

log_msg("Derived variable creation complete", "INFO")
track_sample(df_mapped, "derived variables")

# =============================================================================
# STEP 7: DATA QUALITY CHECKS
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 7: Running data quality checks", "INFO")
log_msg("-----------------------------------------------------------------", "INFO")

quality_issues <- list()

for (check_name in names(schema$quality_checks)) {
  check_spec <- schema$quality_checks[[check_name]]
  log_msg(glue("  Checking: {check_spec$description}"), "DEBUG")
  
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
      n_total <- sum(!is.na(check_result))  # Only count non-NA
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
      }
      
    }, error = function(e) {
      log_msg(glue("    ✗ Check failed to evaluate: {e$message}"), "ERROR")
    })
  }
}

if (length(quality_issues) == 0) {
  log_msg("  All quality checks passed", "INFO")
} else {
  log_msg(glue("  {length(quality_issues)} quality issues identified (see log)"), "WARN")
}

# =============================================================================
# STEP 8: SAVE OUTPUT
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 8: Saving output", "INFO")
log_msg("-----------------------------------------------------------------", "INFO")

# Ensure output directory exists
fs::dir_create("data/interim")

# Save main dataset (all rows, with derived variables and flags)
output_file <- "data/interim/deals_ingested.rds"
saveRDS(df_mapped, output_file)
log_msg(glue("✓ Saved: {output_file}"), "INFO")

# Save column mapping for documentation
readr::write_csv(mapping_table, "data/interim/deals_column_mapping.csv")
log_msg("✓ Saved: data/interim/deals_column_mapping.csv", "INFO")

# Save quality issues if any
if (length(quality_issues) > 0) {
  quality_df <- bind_rows(quality_issues)
  readr::write_csv(quality_df, "data/interim/deals_quality_issues.csv")
  log_msg("✓ Saved: data/interim/deals_quality_issues.csv", "INFO")
}

# =============================================================================
# STEP 9: SUMMARY STATISTICS FOR DOCUMENTATION
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 9: Generating summary statistics", "INFO")
log_msg("-----------------------------------------------------------------", "INFO")

# Comprehensive summary for thesis appendix
summary_stats <- list(
  # Metadata
  input_file = basename(raw_path),
  processing_date = as.character(Sys.time()),
  schema_version = schema_path,
  
  # Sample size cascade
  n_raw = n_raw,
  n_empty_removed = n_empty,
  n_duplicates_removed = n_before - n_after,
  n_final = nrow(df_mapped),
  
  # Column mapping
  n_cols_raw = ncol(df_raw),
  n_cols_mapped = length(mapped_cols),
  n_required_missing = n_required_missing,
  
  # Outcome distribution
  n_completed = sum(df_mapped$deal_outcome_terminal == "completed", na.rm = TRUE),
  n_withdrawn = sum(df_mapped$deal_outcome_terminal == "withdrawn", na.rm = TRUE),
  n_completed_assumed = sum(df_mapped$deal_outcome_terminal == "completed_assumed", na.rm = TRUE),
  n_other = sum(df_mapped$deal_outcome_terminal == "other", na.rm = TRUE),
  
  # Sample filter readiness
  n_terminal_outcome = sum(df_mapped$flag_terminal_outcome, na.rm = TRUE),
  n_has_announce_date = sum(df_mapped$flag_has_announce_date, na.rm = TRUE),
  n_control_transfer = sum(df_mapped$flag_control_transfer, na.rm = TRUE),
  n_has_offer_price = sum(df_mapped$flag_has_offer_price, na.rm = TRUE),
  n_us_listed = sum(df_mapped$flag_us_listed, na.rm = TRUE),
  
  # Quality issues
  n_quality_issues = length(quality_issues),
  
  # Time period coverage
  min_announce_date = as.character(min(df_mapped$announce_date, na.rm = TRUE)),
  max_announce_date = as.character(max(df_mapped$announce_date, na.rm = TRUE)),
  
  # Industry coverage
  n_with_sic = sum(!is.na(df_mapped$target_sic_primary)),
  top_industries = df_mapped %>%
    count(industry_broad) %>%
    arrange(desc(n)) %>%
    head(5) %>%
    mutate(label = glue("{industry_broad} (n={n})")) %>%
    pull(label) %>%
    paste(collapse = "; ")
)

# Save as JSON for programmatic access
jsonlite::write_json(
  summary_stats,
  "data/interim/deals_ingestion_summary.json",
  pretty = TRUE,
  auto_unbox = TRUE
)
log_msg("✓ Saved: data/interim/deals_ingestion_summary.json", "INFO")

# Save as human-readable text for thesis appendix
summary_text <- c(
  "=======================================================================",
  "M&A DEAL DATA INGESTION SUMMARY",
  "=======================================================================",
  "",
  "INPUT:",
  glue("  File: {summary_stats$input_file}"),
  glue("  Processing date: {summary_stats$processing_date}"),
  "",
  "SAMPLE SIZE CASCADE:",
  glue("  Raw data loaded: {summary_stats$n_raw} rows"),
  glue("  Empty rows removed: {summary_stats$n_empty_removed}"),
  glue("  Duplicates removed: {summary_stats$n_duplicates_removed}"),
  glue("  Final dataset: {summary_stats$n_final} rows"),
  "",
  "COLUMN MAPPING:",
  glue("  Raw columns: {summary_stats$n_cols_raw}"),
  glue("  Mapped to schema: {summary_stats$n_cols_mapped}"),
  glue("  Required missing: {summary_stats$n_required_missing}"),
  "",
  "DEAL OUTCOME DISTRIBUTION:",
  glue("  Completed: {summary_stats$n_completed} ({round(100*summary_stats$n_completed/summary_stats$n_final,1)}%)"),
  glue("  Withdrawn: {summary_stats$n_withdrawn} ({round(100*summary_stats$n_withdrawn/summary_stats$n_final,1)}%)"),
  glue("  Completed Assumed: {summary_stats$n_completed_assumed} ({round(100*summary_stats$n_completed_assumed/summary_stats$n_final,1)}%)"),
  glue("  Other: {summary_stats$n_other} ({round(100*summary_stats$n_other/summary_stats$n_final,1)}%)"),
  "",
  "SAMPLE FILTER READINESS:",
  glue("  Terminal outcome (Completed/Withdrawn): {summary_stats$n_terminal_outcome}"),
  glue("  Has announcement date: {summary_stats$n_has_announce_date}"),
  glue("  Control transfer indicator: {summary_stats$n_control_transfer}"),
  glue("  Has offer price (premium analysis): {summary_stats$n_has_offer_price}"),
  glue("  US listed: {summary_stats$n_us_listed}"),
  "",
  "TIME PERIOD:",
  glue("  Announcement dates: {summary_stats$min_announce_date} to {summary_stats$max_announce_date}"),
  "",
  "INDUSTRY COVERAGE:",
  glue("  Deals with SIC code: {summary_stats$n_with_sic} ({round(100*summary_stats$n_with_sic/summary_stats$n_final,1)}%)"),
  glue("  Top 5 industries: {summary_stats$top_industries}"),
  "",
  "DATA QUALITY:",
  glue("  Quality checks failed: {summary_stats$n_quality_issues}"),
  if (summary_stats$n_quality_issues > 0) "  See data/interim/deals_quality_issues.csv for details" else "",
  "",
  "OUTPUT:",
  glue("  Main dataset: data/interim/deals_ingested.rds"),
  glue("  Column mapping: data/interim/deals_column_mapping.csv"),
  glue("  Processing log: data/interim/deals_ingestion_log.txt"),
  "",
  "NEXT STEPS:",
  "  1. Apply sample restrictions (see Sample_Blueprint_ruleset.pdf)",
  "  2. Collapse competing bid episodes (DL-06)",
  "  3. Match to 10-K filings via CIK",
  "  4. Construct NLP indices from 10-K text",
  "  5. Merge with financial controls and construct premium variable",
  "",
  "======================================================================="
)

writeLines(summary_text, "data/interim/deals_ingestion_summary.txt")
log_msg("✓ Saved: data/interim/deals_ingestion_summary.txt", "INFO")

# =============================================================================
# COMPLETION
# =============================================================================

log_msg("", "INFO")
log_msg("=================================================================", "INFO")
log_msg("INGESTION COMPLETE", "INFO")
log_msg("=================================================================", "INFO")
log_msg(glue("Final output: {nrow(df_mapped)} deals with derived variables"), "INFO")
log_msg(glue("Output file: {output_file}"), "INFO")
log_msg(glue("End time: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"), "INFO")

close(log_conn)

# Print summary to console
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("M&A DEAL DATA INGESTION COMPLETE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat(glue("Input:  {basename(raw_path)}"), "\n")
cat(glue("Output: {output_file}"), "\n")
cat(glue("Deals:  {n_raw} → {nrow(df_mapped)} (after deduplication and cleaning)"), "\n")
cat(glue("Period: {summary_stats$min_announce_date} to {summary_stats$max_announce_date}"), "\n")
cat("\n")
cat("Outcome distribution:\n")
cat(glue("  Completed:         {summary_stats$n_completed}"), "\n")
cat(glue("  Withdrawn:         {summary_stats$n_withdrawn}"), "\n")
cat(glue("  Completed Assumed: {summary_stats$n_completed_assumed}"), "\n")
cat(glue("  Other:             {summary_stats$n_other}"), "\n")
cat("\n")
cat("See data/interim/deals_ingestion_summary.txt for full details\n")
cat(paste(rep("=", 70), collapse = ""), "\n")

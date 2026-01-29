# =============================================================================
# src/10_ingest/read_deals_v3_flexible.R
# =============================================================================
# FLEXIBLE M&A deal data ingestion with auto-detection
#
# Key features:
# - Auto-detects column names from multiple possible variations
# - Handles missing optional fields gracefully  
# - Creates sample filter flags (doesn't drop rows)
# - Generates comprehensive documentation
# - Works with any SDC/Refinitiv export format
#
# Usage:
#   source("src/10_ingest/read_deals_v3_flexible.R")
#
# Outputs:
#   - data/interim/deals_ingested.rds
#   - data/interim/ingestion_log.txt
#   - data/interim/column_mapping.csv
#   - data/interim/ingestion_summary.json
# =============================================================================

suppressPackageStartupMessages({
  library(readxl)
  library(yaml)
  library(dplyr)
  library(stringr)
  library(lubridate)
  library(tidyr)
  library(janitor)
  library(glue)
  library(jsonlite)
})

# =============================================================================
# LOGGING SETUP
# =============================================================================

log_file <- "data/interim/ingestion_log.txt"
dir.create("data/interim", recursive = TRUE, showWarnings = FALSE)
log_conn <- file(log_file, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  message(full_msg)
}

log_msg(paste(rep("=", 65), collapse = ""), "INFO")
log_msg("FLEXIBLE M&A DATA INGESTION - AUTO-DETECTION MODE", "INFO")
log_msg(paste(rep("=", 65), collapse = ""), "INFO")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

# Find best matching column name from list of alternatives
find_column <- function(df, possible_names, required = FALSE, field_name = "unknown") {
  
  # Clean all column names and possible names for matching
  df_cols_clean <- str_to_lower(str_replace_all(names(df), "[^a-z0-9]", ""))
  possible_clean <- str_to_lower(str_replace_all(possible_names, "[^a-z0-9]", ""))
  
  # Try exact match first
  for (i in seq_along(possible_clean)) {
    idx <- which(df_cols_clean == possible_clean[i])
    if (length(idx) > 0) {
      matched_col <- names(df)[idx[1]]
      log_msg(glue("  ✓ {field_name} <- {matched_col}"), "DEBUG")
      return(matched_col)
    }
  }
  
  # Try partial match (contains)
  for (i in seq_along(possible_clean)) {
    idx <- which(str_detect(df_cols_clean, possible_clean[i]))
    if (length(idx) > 0) {
      matched_col <- names(df)[idx[1]]
      log_msg(glue("  ~ {field_name} <- {matched_col} (partial match)"), "DEBUG")
      return(matched_col)
    }
  }
  
  # Not found
  if (required) {
    log_msg(glue("  ✗ {field_name} [REQUIRED] <- NOT FOUND"), "ERROR")
  } else {
    log_msg(glue("  - {field_name} [optional] <- not found"), "DEBUG")
  }
  
  return(NA_character_)
}

# Parse multiple date formats
parse_flexible_date <- function(x) {
  formats <- c("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d", 
               "%d-%b-%Y", "%d-%B-%Y", "%Y/%m/%d")
  
  for (fmt in formats) {
    result <- suppressWarnings(as.Date(as.character(x), format = fmt))
    if (sum(!is.na(result)) > length(result) * 0.5) {
      return(result)
    }
  }
  
  # Try lubridate as fallback
  suppressWarnings(lubridate::as_date(x))
}

# =============================================================================
# STEP 1: LOAD RAW DATA
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 1: Loading raw data", "INFO")
log_msg(paste(rep("-", 65), collapse = ""), "INFO")

# Find Excel file
raw_files <- list.files("data/raw", pattern = "\\.xlsx$", full.names = TRUE)
if (length(raw_files) == 0) {
  stop("No .xlsx files found in data/raw/")
}

raw_path <- raw_files[1]
log_msg(glue("Input: {basename(raw_path)}"), "INFO")

# Load data
df_raw <- read_excel(raw_path, sheet = 1, guess_max = 10000)
log_msg(glue("Loaded: {nrow(df_raw)} rows × {ncol(df_raw)} columns"), "INFO")

# Clean column names
df_raw <- clean_names(df_raw)
log_msg(glue("Sample columns: {paste(head(names(df_raw), 5), collapse = ', ')}"), "DEBUG")

# =============================================================================
# STEP 2: AUTO-DETECT AND MAP COLUMNS
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 2: Auto-detecting column mappings", "INFO")
log_msg(paste(rep("-", 65), collapse = ""), "INFO")

# Define all possible column name variations
col_map <- list(
  
  # Core identifiers
  deal_id = c("sdc_deal_no", "deal_number", "deal_id", "dealid", "transaction_id"),
  
  target_name = c("target_full_name", "target_name", "target_company_name", 
                  "targetname", "target"),
  
  acquirer_name = c("acquiror_full_name", "acquiror_name", "acquirer_name", 
                    "acquirername", "buyer_name"),
  
  # Tickers and identifiers
  target_ticker = c("target_primary_ticker_symbol", "target_ticker_symbol", 
                    "target_ticker", "ticker", "target_trading_symbol"),
  
  target_cusip = c("target_cusip", "cusip"),
  
  target_isin = c("target_isin_number", "target_isin", "isin"),
  
  # Dates
  announce_date = c("date_announced", "announced_date", "announce_date", 
                    "announcement_date", "date_ann"),
  
  complete_date = c("date_effective_unconditional", "completed_date", 
                    "complete_date", "completion_date", "effective_date", 
                    "date_eff"),
  
  withdrawn_date = c("date_withdrawn", "withdrawn_date", "termination_date"),
  
  # Deal characteristics
  deal_status = c("deal_status", "status", "transaction_status"),
  
  deal_type = c("deal_type", "transaction_type", "type", "form"),
  
  payment_method = c("deal_method_of_payment", "consideration_structure", 
                     "payment_method", "consideration_type"),
  
  # Values
  deal_value = c("rank_value_including_net_debt_of_target_usd_millions",
                 "deal_value_th_eur", "deal_value_eur_th", "transaction_value_eur",
                 "deal_value_mil", "value_usd_mil", "deal_value"),
  
  offer_price = c("offer_price_eur", "price_per_share_eur", "offer_price",
                  "price_per_share"),
  
  # Stakes
  initial_stake_pct = c("percent_of_shares_owned_before_transaction",
                        "initial_stake_percent", "initial_stake_pct",
                        "initial_ownership_pct", "acq_own_before"),
  
  final_stake_pct = c("percent_of_shares_acquired", "final_stake_percent",
                      "final_stake_pct", "final_ownership_pct",
                      "percent_acquired", "acq_own_after"),
  
  # Industry codes
  target_sic = c("target_primary_us_sic_code", "target_us_sic_code_s",
                 "target_sic", "sic_code", "target_sic_code"),
  
  acquirer_sic = c("acquiror_primary_us_sic_code", "acquiror_us_sic_code_s",
                   "acquirer_sic", "acquiror_sic", "acq_sic_code"),
  
  # Geography
  target_nation = c("target_nation", "target_country", "target_country_code"),
  
  target_exchange = c("target_stock_exchange_s_listed", "target_exchange",
                      "exchange", "target_primary_stock_exchange"),
  
  acquirer_country = c("acquiror_nation", "acquiror_country_code",
                       "acquirer_country", "buyer_country"),
  
  # Other useful fields
  target_public_status = c("target_public_status", "public_status"),
  
  target_macro_industry = c("target_macro_industry", "macro_industry"),
  
  target_mid_industry = c("target_mid_industry", "mid_industry")
)

# Apply mapping
mapping_results <- tibble(
  standard_name = names(col_map),
  source_column = NA_character_,
  found = FALSE
)

log_msg("Column mapping results:", "INFO")

for (i in seq_along(col_map)) {
  std_name <- names(col_map)[i]
  possible <- col_map[[i]]
  
  # Determine if required
  required <- std_name %in% c("deal_id", "target_name", "announce_date")
  
  matched <- find_column(df_raw, possible, required = required, field_name = std_name)
  
  if (!is.na(matched)) {
    mapping_results$source_column[i] <- matched
    mapping_results$found[i] <- TRUE
  }
}

# Check critical fields
critical_fields <- c("deal_id", "target_name", "announce_date")
missing_critical <- mapping_results %>%
  filter(standard_name %in% critical_fields, !found) %>%
  pull(standard_name)

if (length(missing_critical) > 0) {
  log_msg(glue("ERROR: Missing critical fields: {paste(missing_critical, collapse = ', ')}"), "ERROR")
  log_msg("Available columns:", "INFO")
  log_msg(paste(names(df_raw), collapse = "\n"), "INFO")
  stop("Cannot proceed without critical fields")
}

# Save mapping
write.csv(mapping_results, "data/interim/column_mapping.csv", row.names = FALSE)
log_msg(glue("Mapped {sum(mapping_results$found)}/{nrow(mapping_results)} columns"), "INFO")

# =============================================================================
# STEP 3: BUILD STANDARDIZED DATASET
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 3: Building standardized dataset", "INFO")
log_msg(paste(rep("-", 65), collapse = ""), "INFO")

# Create base dataset with mapped columns
df <- tibble(.rows = nrow(df_raw))

for (i in 1:nrow(mapping_results)) {
  std_name <- mapping_results$standard_name[i]
  src_col <- mapping_results$source_column[i]
  
  if (!is.na(src_col) && src_col %in% names(df_raw)) {
    df[[std_name]] <- df_raw[[src_col]]
  } else {
    df[[std_name]] <- NA
  }
}

log_msg(glue("Created dataset with {ncol(df)} standardized columns"), "INFO")

# =============================================================================
# STEP 4: DATA TYPE CONVERSION
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 4: Converting data types", "INFO")
log_msg(paste(rep("-", 65), collapse = ""), "INFO")

# Dates
date_cols <- c("announce_date", "complete_date", "withdrawn_date")
for (col in date_cols) {
  if (col %in% names(df)) {
    df[[col]] <- parse_flexible_date(df[[col]])
    n_parsed <- sum(!is.na(df[[col]]))
    log_msg(glue("  {col}: {n_parsed}/{nrow(df)} dates parsed"), "DEBUG")
  }
}

# Numeric
numeric_cols <- c("deal_value", "offer_price", "initial_stake_pct", "final_stake_pct")
for (col in numeric_cols) {
  if (col %in% names(df) && !all(is.na(df[[col]]))) {
    df[[col]] <- suppressWarnings(as.numeric(df[[col]]))
  }
}

# Character (trim whitespace)
char_cols <- c("deal_id", "target_name", "acquirer_name", "target_ticker",
               "deal_status", "deal_type", "payment_method")
for (col in char_cols) {
  if (col %in% names(df) && !all(is.na(df[[col]]))) {
    df[[col]] <- str_trim(as.character(df[[col]]))
  }
}

log_msg("Type conversion complete", "INFO")

# =============================================================================
# STEP 5: REMOVE DUPLICATES
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 5: Deduplication", "INFO")
log_msg(paste(rep("-", 65), collapse = ""), "INFO")

n_before <- nrow(df)
df <- df %>% distinct(deal_id, .keep_all = TRUE)
n_after <- nrow(df)
n_dupes <- n_before - n_after

log_msg(glue("Removed {n_dupes} duplicates"), if (n_dupes > 0) "WARN" else "INFO")
log_msg(glue("Final: {n_after} unique deals"), "INFO")

# =============================================================================
# STEP 6: CREATE DERIVED VARIABLES
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 6: Creating derived variables", "INFO")
log_msg(paste(rep("-", 65), collapse = ""), "INFO")

df <- df %>%
  mutate(
    # Outcome indicators
    deal_completed = case_when(
      str_to_lower(str_trim(deal_status)) == "completed" ~ 1L,
      str_to_lower(str_trim(deal_status)) == "withdrawn" ~ 0L,
      TRUE ~ NA_integer_
    ),
    
    deal_outcome_terminal = case_when(
      str_to_lower(str_trim(deal_status)) %in% c("completed", "withdrawn") ~ TRUE,
      TRUE ~ FALSE
    ),
    
    time_to_close = as.integer(
      coalesce(complete_date, withdrawn_date) - announce_date
    ),
    
    # Industry classifications
    target_sic_primary = str_extract(target_sic, "^\\d{4}"),
    target_sic_2digit = str_sub(target_sic_primary, 1, 2),
    
    # Time indicators
    year_announced = year(announce_date),
    
    # Payment method standardization
    payment_method_clean = case_when(
      str_detect(str_to_lower(payment_method), "^cash$|100%.*cash|all.*cash") ~ "Cash",
      str_detect(str_to_lower(payment_method), "^shares|^stock|100%.*stock|all.*stock") ~ "Stock",
      str_detect(str_to_lower(payment_method), "cash.*stock|stock.*cash|mixed") ~ "Mixed",
      !is.na(payment_method) ~ "Other",
      TRUE ~ NA_character_
    ),
    
    # Fixed effects identifiers
    industry_year = paste0(target_sic_2digit, "_", year_announced)
  )

log_msg("Created outcome and classification variables", "INFO")

# =============================================================================
# STEP 7: CREATE SAMPLE FILTER FLAGS
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 7: Creating sample filter flags", "INFO")
log_msg(paste(rep("-", 65), collapse = ""), "INFO")

df <- df %>%
  mutate(
    # Core filters
    flag_has_deal_id = !is.na(deal_id),
    flag_has_target_name = !is.na(target_name),
    flag_has_announce_date = !is.na(announce_date),
    flag_terminal_outcome = deal_outcome_terminal,
    
    # Deal characteristics
    flag_control_transfer = case_when(
      is.na(final_stake_pct) ~ NA,
      final_stake_pct >= 50 ~ TRUE,
      TRUE ~ FALSE
    ),
    
    # Geography
    flag_us_target = case_when(
      is.na(target_nation) ~ NA,
      str_detect(str_to_upper(target_nation), "US$|USA|UNITED STATES") ~ TRUE,
      TRUE ~ FALSE
    ),
    
    # Data quality
    flag_valid_dates = case_when(
      is.na(announce_date) ~ FALSE,
      !is.na(complete_date) & complete_date < announce_date ~ FALSE,
      !is.na(withdrawn_date) & withdrawn_date < announce_date ~ FALSE,
      TRUE ~ TRUE
    ),
    
    flag_positive_values = case_when(
      !is.na(deal_value) & deal_value < 0 ~ FALSE,
      !is.na(offer_price) & offer_price < 0 ~ FALSE,
      TRUE ~ TRUE
    )
  )

# Count flags
flag_summary <- df %>%
  summarise(across(starts_with("flag_"), ~sum(.x, na.rm = TRUE))) %>%
  pivot_longer(everything(), names_to = "flag", values_to = "count") %>%
  mutate(pct = round(100 * count / nrow(df), 1))

log_msg("Sample filter flag summary:", "INFO")
for (i in 1:nrow(flag_summary)) {
  log_msg(glue("  {flag_summary$flag[i]}: {flag_summary$count[i]} ({flag_summary$pct[i]}%)"), "DEBUG")
}

# =============================================================================
# STEP 8: SAVE OUTPUT
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 8: Saving outputs", "INFO")
log_msg(paste(rep("-", 65), collapse = ""), "INFO")

# Main output
output_path <- "data/interim/deals_ingested.rds"
saveRDS(df, output_path)
log_msg(glue("Saved: {output_path}"), "INFO")

# Summary statistics
summary_stats <- list(
  processing_date = as.character(Sys.Date()),
  input_file = basename(raw_path),
  n_rows_raw = nrow(df_raw),
  n_rows_final = nrow(df),
  n_duplicates_removed = n_dupes,
  columns_mapped = sum(mapping_results$found),
  columns_total = nrow(mapping_results),
  
  date_range = list(
    min = as.character(min(df$announce_date, na.rm = TRUE)),
    max = as.character(max(df$announce_date, na.rm = TRUE))
  ),
  
  deal_status_counts = df %>%
    count(deal_status, sort = TRUE) %>%
    head(10) %>%
    {setNames(as.list(.$n), .$deal_status)},
  
  flag_counts = setNames(as.list(flag_summary$count), flag_summary$flag)
)

# Save JSON
summary_path <- "data/interim/ingestion_summary.json"
write_json(summary_stats, summary_path, pretty = TRUE, auto_unbox = TRUE)
log_msg(glue("Saved: {summary_path}"), "INFO")

# =============================================================================
# FINALIZE
# =============================================================================

log_msg("", "INFO")
log_msg(paste(rep("=", 65), collapse = ""), "INFO")
log_msg("INGESTION COMPLETE", "INFO")
log_msg(glue("Final dataset: {nrow(df)} deals, {ncol(df)} columns"), "INFO")
log_msg(paste(rep("=", 65), collapse = ""), "INFO")

close(log_conn)

# Print summary to console
cat("\n")
cat("╔═══════════════════════════════════════════════════════════════╗\n")
cat("║              INGESTION COMPLETE - SUMMARY                      ║\n")
cat("╠═══════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Input file:        %-42s ║\n", basename(raw_path)))
cat(sprintf("║ Rows processed:    %-42d ║\n", nrow(df)))
cat(sprintf("║ Columns mapped:    %-42s ║\n", 
            paste0(sum(mapping_results$found), "/", nrow(mapping_results))))
cat(sprintf("║ Duplicates removed: %-41d ║\n", n_dupes))
cat("╠═══════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Date range:        %s to %s       ║\n", 
            format(min(df$announce_date, na.rm = TRUE), "%Y"),
            format(max(df$announce_date, na.rm = TRUE), "%Y")))
cat("╠═══════════════════════════════════════════════════════════════╣\n")
cat("║ Output:            data/interim/deals_ingested.rds             ║\n")
cat("║ Log:               data/interim/ingestion_log.txt              ║\n")
cat("║ Mapping:           data/interim/column_mapping.csv             ║\n")
cat("╚═══════════════════════════════════════════════════════════════╝\n")
cat("\n")
cat("✓ Ready for sample restriction step\n")
cat("  Run: source('src/20_clean/apply_sample_restrictions.R')\n\n")

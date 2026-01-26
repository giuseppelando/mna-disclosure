# =============================================================================
# src/10_ingest/read_deals.R
# =============================================================================
# Reads raw M&A deal data from Excel and performs initial cleaning
# Output: data/interim/deals_clean.csv

suppressPackageStartupMessages({
  library(readxl)
  library(janitor)
  library(dplyr)
  library(stringr)
  library(lubridate)
  library(readr)
  library(rlang)
})

# Find first xlsx in data/raw
raw_files <- list.files("data/raw", pattern = "\\.xlsx$", full.names = TRUE)
stopifnot(length(raw_files) >= 1)
raw_path <- raw_files[1]

message("Reading: ", basename(raw_path))

# Read and clean column names
df <- readxl::read_excel(raw_path, sheet = 1) %>%
  janitor::clean_names()

message("  Raw dimensions: ", nrow(df), " rows × ", ncol(df), " columns")

# Remove completely empty rows
df <- df %>%
  filter(if_any(everything(), ~ !is.na(.)))

message("  After removing empty rows: ", nrow(df), " rows")

# =============================================================================
# SAFE RENAME HELPER
# =============================================================================
safe_rename <- function(.data, old, new) {
  if (old %in% names(.data)) {
    dplyr::rename(.data, !!new := !!sym(old))
  } else {
    .data
  }
}

# Remove "unnamed" columns
df <- df %>%
  dplyr::select(!dplyr::starts_with("unnamed"))

# =============================================================================
# COLUMN MAPPING
# =============================================================================
# Map columns to standardized names

df <- df %>%
  # Core identifiers
  safe_rename("deal_number", "deal_id") %>%
  safe_rename("target_name", "target_name") %>%
  safe_rename("acquiror_name", "acquirer_name") %>%
  safe_rename("target_ticker_symbol", "target_ticker") %>%
  safe_rename("target_isin_number", "target_isin") %>%
  
  # Dates
  safe_rename("announced_date", "announce_date") %>%
  safe_rename("completed_date", "complete_date") %>%
  safe_rename("withdrawn_date", "withdrawn_date") %>%
  safe_rename("expected_completion_date", "expected_complete_date") %>%
  safe_rename("assumed_completion_date", "assumed_complete_date") %>%
  
  # Deal characteristics
  safe_rename("deal_status", "deal_status") %>%
  safe_rename("deal_type", "deal_type") %>%
  safe_rename("deal_method_of_payment", "payment_method") %>%
  safe_rename("deal_value_th_eur", "deal_value_eur_th") %>%
  safe_rename("offer_price_eur", "offer_price_eur") %>%
  safe_rename("initial_stake_percent", "initial_stake_pct") %>%
  safe_rename("final_stake_percent", "final_stake_pct") %>%
  
  # Industry/geography
  safe_rename("target_us_sic_code_s", "target_sic") %>%
  safe_rename("target_stock_exchange_s_listed", "target_exchange") %>%
  safe_rename("acquiror_country_code", "acquirer_country")

# Create deal_id if missing
if (!"deal_id" %in% names(df)) {
  df <- df %>%
    mutate(deal_id = row_number())
}

# =============================================================================
# DATA TYPE CONVERSIONS
# =============================================================================

# Clean numeric columns (remove commas, convert)
clean_num <- function(x) {
  x %>%
    as.character() %>%
    stringr::str_replace_all(",", "") %>%
    stringr::str_replace_all("\\*", "") %>%  # Remove asterisks
    as.numeric()
}

# Convert deal value
if ("deal_value_eur_th" %in% names(df)) {
  df <- df %>%
    mutate(deal_value_eur_th = clean_num(deal_value_eur_th))
}

# Convert offer price
if ("offer_price_eur" %in% names(df)) {
  df <- df %>%
    mutate(offer_price_eur = clean_num(offer_price_eur))
}

# Convert stakes
if ("initial_stake_pct" %in% names(df)) {
  df <- df %>%
    mutate(initial_stake_pct = clean_num(initial_stake_pct))
}

if ("final_stake_pct" %in% names(df)) {
  df <- df %>%
    mutate(final_stake_pct = clean_num(final_stake_pct))
}

# Convert dates
date_cols <- c("announce_date", "complete_date", "withdrawn_date", 
               "expected_complete_date", "assumed_complete_date")

for (col in date_cols) {
  if (col %in% names(df)) {
    df <- df %>%
      mutate(!!sym(col) := as.Date(!!sym(col)))
  }
}

# =============================================================================
# DERIVED VARIABLES
# =============================================================================

# Completion dummy
if ("deal_status" %in% names(df)) {
  df <- df %>%
    mutate(
      deal_completed = if_else(
        str_to_lower(str_trim(deal_status)) == "completed", 
        1L, 
        0L, 
        missing = 0L
      )
    )
}

# Clean ticker (uppercase, trim)
if ("target_ticker" %in% names(df)) {
  df <- df %>%
    mutate(target_ticker = toupper(trimws(target_ticker)))
}

# Extract first SIC code (many deals have multiple)
if ("target_sic" %in% names(df)) {
  df <- df %>%
    mutate(
      target_sic_primary = str_extract(target_sic, "^\\d{4}"),
      target_sic_2digit = str_sub(target_sic_primary, 1, 2)
    )
}

# =============================================================================
# FILTERING
# =============================================================================

# Keep only rows with minimal useful information
df_clean <- df %>%
  filter(
    !is.na(target_ticker) | !is.na(target_name)
  )

message("  After filtering: ", nrow(df_clean), " rows")

# =============================================================================
# SAVE OUTPUT
# =============================================================================

fs::dir_create("data/interim")
readr::write_csv(df_clean, "data/interim/deals_clean.csv")

message("✓ Saved → data/interim/deals_clean.csv (", nrow(df_clean), " rows)")
message("  Columns: ", paste(names(df_clean), collapse = ", "))

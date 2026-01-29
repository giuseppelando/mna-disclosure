# =============================================================================
# src/10_ingest/map_tickers_to_cik.R (refined for standardized pipeline)
# =============================================================================
# Map target ticker symbols to SEC CIK identifiers
#
# Purpose:
#   Add EDGAR CIK identifiers to deals by matching ticker symbols against
#   the SEC's company tickers master file. The CIK is required to retrieve
#   10-K filings in subsequent pipeline steps.
#
# Input:
#   data/interim/deals_sample_restricted.rds (from apply_sample_restrictions.R)
#
# Output:
#   data/interim/deals_with_cik.rds (deals with CIK identifiers added)
#   data/interim/cik_mapping_log.txt (detailed processing log)
#   data/interim/cik_mapping_summary.json (match statistics)
#   data/interim/ticker_cik_lookup.csv (SEC mapping table for reference)
#
# Technical Notes:
#   - Downloads SEC company tickers file (updated regularly by SEC)
#   - Performs case-insensitive ticker matching
#   - CIKs are zero-padded to 10 digits per EDGAR convention
#   - Unmatched tickers are retained with NA for downstream diagnosis
#
# Usage:
#   Rscript src/10_ingest/map_tickers_to_cik.R
#
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(jsonlite)
  library(httr)
  library(stringr)
  library(fs)
  library(glue)
})

# =============================================================================
# CONFIGURATION
# =============================================================================

# Input file from sample restriction
INPUT_FILE <- "data/processed/deals_restricted.rds"

# Output files
OUTPUT_FILE <- "data/interim/deals_with_cik.rds"
LOG_FILE <- "data/interim/cik_mapping_log.txt"
SUMMARY_FILE <- "data/interim/cik_mapping_summary.json"
LOOKUP_FILE <- "data/interim/ticker_cik_lookup.csv"

# SEC company tickers JSON endpoint
SEC_TICKERS_URL <- "https://www.sec.gov/files/company_tickers.json"

# User agent for SEC requests (required by SEC guidelines)
# IMPORTANT: Update with your actual email before running on final data
USER_AGENT <- "mna-disclosure-research giuseppe.lando@studbocconi.it"

# =============================================================================
# SETUP
# =============================================================================

deals <- readRDS("data/processed/deals_restricted.rds")
if (!("target_cik" %in% names(deals))) {
  deals <- deals %>% mutate(target_cik = NA_character_)
}

# Ensure output directory exists
fs::dir_create(dirname(OUTPUT_FILE))

# Set up logging
log_conn <- file(LOG_FILE, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  message(full_msg)
}

log_msg("=================================================================", "INFO")
log_msg("CIK MAPPING (Ticker to CIK Identifier)", "INFO")
log_msg("=================================================================", "INFO")
log_msg(glue("Input: {INPUT_FILE}"), "INFO")
log_msg(glue("Output: {OUTPUT_FILE}"), "INFO")
log_msg(glue("Start time: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"), "INFO")

# =============================================================================
# LOAD RESTRICTED DEALS
# =============================================================================

log_msg("", "INFO")
log_msg("Loading restricted deals dataset...", "INFO")

if (!file.exists(INPUT_FILE)) {
  log_msg(glue("Input file not found: {INPUT_FILE}"), "ERROR")
  stop("Input file missing. Run apply_sample_restrictions.R first.")
}

deals <- readRDS(INPUT_FILE)

n_deals <- nrow(deals)
log_msg(glue("Loaded: {n_deals} deals"), "INFO")

# Verify target_ticker column exists
if (!"target_ticker" %in% names(deals)) {
  log_msg("Column 'target_ticker' not found in input data", "ERROR")
  stop("Input data missing required column")
}

# Count how many deals have non-missing tickers
n_with_ticker <- sum(!is.na(deals$target_ticker) & deals$target_ticker != "")
n_missing_ticker <- n_deals - n_with_ticker
pct_with_ticker <- round(100 * n_with_ticker / n_deals, 1)

log_msg(glue("Deals with ticker: {n_with_ticker} ({pct_with_ticker}%)"), "INFO")
log_msg(glue("Deals without ticker: {n_missing_ticker}"), "INFO")

# =============================================================================
# DOWNLOAD SEC COMPANY TICKERS MAPPING
# =============================================================================

log_msg("", "INFO")
log_msg("Retrieving SEC company tickers mapping...", "INFO")
log_msg(glue("URL: {SEC_TICKERS_URL}"), "DEBUG")

# Download the SEC mapping file
sec_response <- tryCatch({
  httr::GET(
    SEC_TICKERS_URL,
    httr::add_headers(`User-Agent` = USER_AGENT),
    httr::timeout(30)
  )
}, error = function(e) {
  log_msg(glue("Failed to download SEC mapping: {e$message}"), "ERROR")
  stop("SEC mapping download failed")
})

# Check HTTP status
if (httr::http_error(sec_response)) {
  status <- httr::status_code(sec_response)
  log_msg(glue("HTTP error {status} when downloading SEC mapping"), "ERROR")
  stop("SEC API returned error status")
}

log_msg("✓ SEC mapping file downloaded successfully", "INFO")

# Parse JSON response
sec_raw <- tryCatch({
  httr::content(sec_response, as = "text", encoding = "UTF-8") %>%
    jsonlite::fromJSON(simplifyDataFrame = TRUE)
}, error = function(e) {
  log_msg(glue("Failed to parse SEC JSON: {e$message}"), "ERROR")
  stop("JSON parsing failed")
})

# The SEC file structure is a list of entries indexed by numbers
# Convert to a proper data frame if needed
if (is.list(sec_raw) && !is.data.frame(sec_raw)) {
  sec_raw <- dplyr::bind_rows(sec_raw)
}

# Verify expected columns exist
required_cols <- c("cik_str", "ticker", "title")
missing_cols <- setdiff(required_cols, names(sec_raw))

if (length(missing_cols) > 0) {
  log_msg(glue("SEC mapping missing expected columns: {paste(missing_cols, collapse = ', ')}"), "ERROR")
  stop("SEC mapping file format has changed")
}

log_msg(glue("SEC mapping contains {nrow(sec_raw)} entries"), "INFO")

# =============================================================================
# PREPARE TICKER-CIK LOOKUP TABLE
# =============================================================================

log_msg("", "INFO")
log_msg("Preparing ticker-CIK lookup table...", "INFO")

# Create standardized lookup table
ticker_cik_lookup <- sec_raw %>%
  transmute(
    # Standardize ticker: uppercase, trimmed
    ticker_clean = toupper(trimws(ticker)),
    
    # Zero-pad CIK to 10 digits (EDGAR convention)
    target_cik = str_pad(
      as.character(cik_str), 
      width = 10, 
      side = "left", 
      pad = "0"
    ),
    
    # Company name from SEC for reference
    sec_company_name = title
  )

# Remove any duplicates (keep first occurrence)
n_before_dedup <- nrow(ticker_cik_lookup)
ticker_cik_lookup <- ticker_cik_lookup %>%
  distinct(ticker_clean, .keep_all = TRUE)
n_after_dedup <- nrow(ticker_cik_lookup)
n_dup_removed <- n_before_dedup - n_after_dedup

if (n_dup_removed > 0) {
  log_msg(glue("Removed {n_dup_removed} duplicate tickers (kept first occurrence)"), "WARN")
}

log_msg(glue("Lookup table: {nrow(ticker_cik_lookup)} unique tickers"), "INFO")

# Save lookup table for reference and manual inspection if needed
readr::write_csv(ticker_cik_lookup, LOOKUP_FILE)
log_msg(glue("✓ Saved lookup table: {LOOKUP_FILE}"), "INFO")

# =============================================================================
# MATCH DEALS TO CIKS
# =============================================================================

log_msg("", "INFO")
log_msg("Matching deals to CIK identifiers...", "INFO")

# Prepare deals for matching (standardize ticker format)
deals_for_matching <- deals %>%
  mutate(
    # Create standardized ticker for matching
    ticker_clean = toupper(trimws(target_ticker))
  )

# Perform left join to add CIK identifiers
deals_with_cik <- deals_for_matching %>%
  left_join(
    ticker_cik_lookup %>% select(ticker_clean, target_cik, sec_company_name),
    by = "ticker_clean"
  ) %>%
  # Remove the temporary ticker_clean column
  select(-ticker_clean)

# =============================================================================
# MATCH STATISTICS AND DIAGNOSTICS
# =============================================================================

log_msg("", "INFO")
log_msg("Computing match statistics...", "INFO")

# Overall match rate
n_matched <- sum(!is.na(deals_with_cik$target_cik))
n_unmatched <- n_deals - n_matched
pct_matched <- round(100 * n_matched / n_deals, 1)

log_msg(glue("Matched to CIK: {n_matched} deals ({pct_matched}%)"), "INFO")
log_msg(glue("Unmatched: {n_unmatched} deals"), "INFO")

# Break down unmatched deals by reason
deals_with_cik <- deals_with_cik %>%
  mutate(
    cik_match_status = case_when(
      !is.na(target_cik) ~ "matched",
      is.na(target_ticker) | target_ticker == "" ~ "no_ticker",
      TRUE ~ "ticker_not_in_sec"
    )
  )

match_status_summary <- deals_with_cik %>%
  count(cik_match_status) %>%
  mutate(pct = round(100 * n / n_deals, 1))

log_msg("Match status breakdown:", "INFO")
for (i in seq_len(nrow(match_status_summary))) {
  row <- match_status_summary[i, ]
  log_msg(glue("  {row$cik_match_status}: {row$n} ({row$pct}%)"), "INFO")
}

# Identify some examples of unmatched tickers for diagnostic purposes
unmatched_tickers <- deals_with_cik %>%
  filter(cik_match_status == "ticker_not_in_sec") %>%
  select(target_ticker, target_name) %>%
  distinct() %>%
  head(10)

if (nrow(unmatched_tickers) > 0) {
  log_msg("", "INFO")
  log_msg("Sample of unmatched tickers (first 10 unique):", "DEBUG")
  for (i in seq_len(nrow(unmatched_tickers))) {
    row <- unmatched_tickers[i, ]
    log_msg(glue("  {row$target_ticker} ({row$target_name})"), "DEBUG")
  }
  log_msg("Note: Tickers may be historical/delisted or have changed", "DEBUG")
}

# =============================================================================
# SAVE OUTPUTS
# =============================================================================

log_msg("", "INFO")
log_msg("Saving outputs...", "INFO")

# Save deals with CIK identifiers
saveRDS(deals_with_cik, OUTPUT_FILE)
log_msg(glue("✓ Saved: {OUTPUT_FILE}"), "INFO")

# Create summary statistics
summary_data <- list(
  input_file = INPUT_FILE,
  output_file = OUTPUT_FILE,
  processing_date = as.character(Sys.time()),
  sec_mapping_url = SEC_TICKERS_URL,
  n_deals = n_deals,
  n_with_ticker = n_with_ticker,
  n_matched = n_matched,
  n_unmatched = n_unmatched,
  pct_matched = pct_matched,
  match_status_breakdown = match_status_summary,
  sample_unmatched_tickers = if(nrow(unmatched_tickers) > 0) {
    unmatched_tickers %>% 
      mutate(ticker = target_ticker, name = target_name) %>%
      select(ticker, name)
  } else {
    data.frame(ticker = character(), name = character())
  }
)

jsonlite::write_json(
  summary_data,
  SUMMARY_FILE,
  pretty = TRUE,
  auto_unbox = TRUE
)
log_msg(glue("✓ Saved: {SUMMARY_FILE}"), "INFO")

# =============================================================================
# COMPLETION
# =============================================================================

log_msg("", "INFO")
log_msg("=================================================================", "INFO")
log_msg("CIK MAPPING COMPLETE", "INFO")
log_msg("=================================================================", "INFO")
log_msg(glue("Deals processed: {n_deals}"), "INFO")
log_msg(glue("Successfully matched: {n_matched} ({pct_matched}%)"), "INFO")
log_msg(glue("Unmatched (no ticker): {sum(match_status_summary$n[match_status_summary$cik_match_status == 'no_ticker'])}"), "INFO")
log_msg(glue("Unmatched (ticker not in SEC): {sum(match_status_summary$n[match_status_summary$cik_match_status == 'ticker_not_in_sec'])}"), "INFO")
log_msg(glue("End time: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"), "INFO")

close(log_conn)

# Print summary to console
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CIK MAPPING COMPLETE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat(glue("Input:  {INPUT_FILE}"), "\n")
cat(glue("Output: {OUTPUT_FILE}"), "\n")
cat(glue("Deals:  {n_deals}"), "\n")
cat(glue("Matched: {n_matched} ({pct_matched}%)"), "\n")
cat("\n")
cat("Match status:\n")
for (i in seq_len(nrow(match_status_summary))) {
  row <- match_status_summary[i, ]
  cat(glue("  {row$cik_match_status}: {row$n} deals ({row$pct}%)"), "\n")
}
cat("\n")

if (n_unmatched > 0) {
  cat("Note: Unmatched deals may have historical/delisted tickers\n")
  cat("      or tickers that have changed since announcement date.\n")
  cat(glue("      See {LOG_FILE} for sample of unmatched tickers.\n"))
}

cat("\n")
cat("Next step: Identify pre-announcement 10-K filings for matched deals\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

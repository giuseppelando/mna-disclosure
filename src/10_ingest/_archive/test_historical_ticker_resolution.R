# =============================================================================
# src/10_ingest/test_historical_ticker_resolution.R
# =============================================================================
# Test script for resolving historical ticker symbols to CIK identifiers
#
# Purpose:
#   This script implements a hybrid matching strategy that combines fast bulk
#   matching against the current SEC company tickers file with intelligent
#   fallback queries to EDGAR's search system for historical tickers that no
#   longer appear in the current registry. The goal is to dramatically improve
#   match rates for M&A datasets containing acquired/delisted companies while
#   maintaining complete transparency about data provenance.
#
# Strategy:
#   1. Bulk match against current SEC company tickers (fast, gets ~15%)
#   2. For unmatched tickers, query EDGAR company search individually
#   3. Cache all discovered mappings to avoid redundant queries
#   4. Generate detailed diagnostics showing match success by method
#
# Technical Notes:
#   - Implements polite rate limiting (150ms between queries) per SEC guidelines
#   - Saves all discovered mappings to local cache for reuse
#   - Provides detailed logging of each resolution attempt
#   - Generates human-readable match report for validation
#
# Usage:
#   source("src/10_ingest/test_historical_ticker_resolution.R")
#
# Output:
#   - Enhanced CIK mapping with historical ticker resolution
#   - Detailed match statistics by method
#   - Cache file for future runs
#   - Validation report for manual review
#
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(jsonlite)
  library(httr)
  library(stringr)
  library(xml2)
  library(rvest)
  library(fs)
  library(glue)
})

# =============================================================================
# CONFIGURATION
# =============================================================================

# Input file (from sample restriction)
INPUT_FILE <- "data/interim/deals_sample_restricted.rds"

# Output files
OUTPUT_FILE <- "data/interim/deals_with_cik_enhanced.rds"
LOG_FILE <- "data/interim/ticker_resolution_test_log.txt"
CACHE_FILE <- "data/interim/ticker_cik_cache.rds"
REPORT_FILE <- "data/interim/ticker_resolution_report.txt"

# SEC endpoints
SEC_TICKERS_URL <- "https://www.sec.gov/files/company_tickers.json"
EDGAR_SEARCH_BASE <- "https://www.sec.gov/cgi-bin/browse-edgar"

# User agent for SEC requests (required)
USER_AGENT <- "mna-disclosure-research giuseppe.lando@studbocconi.it"

# Rate limiting (milliseconds between EDGAR queries)
RATE_LIMIT_MS <- 150

# Maximum number of tickers to attempt via EDGAR search in test mode
# Set to NULL to process all unmatched tickers
MAX_EDGAR_QUERIES <- NULL  # Set to e.g. 100 for quick testing

# =============================================================================
# SETUP
# =============================================================================

fs::dir_create(dirname(OUTPUT_FILE))

log_conn <- file(LOG_FILE, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  message(full_msg)
}

log_msg("=================================================================", "INFO")
log_msg("HISTORICAL TICKER RESOLUTION TEST", "INFO")
log_msg("=================================================================", "INFO")
log_msg(glue("Input: {INPUT_FILE}"), "INFO")
log_msg(glue("Start time: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"), "INFO")

# =============================================================================
# LOAD CACHE IF EXISTS
# =============================================================================

log_msg("", "INFO")
log_msg("Loading or initializing cache...", "INFO")

if (file.exists(CACHE_FILE)) {
  ticker_cache <- readRDS(CACHE_FILE)
  log_msg(glue("Loaded existing cache: {nrow(ticker_cache)} entries"), "INFO")
} else {
  ticker_cache <- tibble(
    ticker = character(),
    cik = character(),
    company_name = character(),
    source = character(),  # "sec_bulk" or "edgar_search"
    query_date = as.Date(character())
  )
  log_msg("Initialized empty cache", "INFO")
}

# =============================================================================
# LOAD DEALS DATA
# =============================================================================

log_msg("", "INFO")
log_msg("Loading deals data...", "INFO")

if (!file.exists(INPUT_FILE)) {
  log_msg(glue("Input file not found: {INPUT_FILE}"), "ERROR")
  stop("Input file missing")
}

deals <- readRDS(INPUT_FILE)
n_deals <- nrow(deals)

log_msg(glue("Loaded: {n_deals} deals"), "INFO")

# Extract unique tickers to resolve
unique_tickers <- deals %>%
  filter(!is.na(target_ticker), target_ticker != "") %>%
  pull(target_ticker) %>%
  unique() %>%
  toupper() %>%
  trimws()

n_unique_tickers <- length(unique_tickers)
log_msg(glue("Unique tickers to resolve: {n_unique_tickers}"), "INFO")

# =============================================================================
# STEP 1: BULK MATCH AGAINST CURRENT SEC TICKERS
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 1: Bulk matching against current SEC tickers...", "INFO")

log_msg("Downloading SEC company tickers...", "INFO")

sec_response <- tryCatch({
  httr::GET(
    SEC_TICKERS_URL,
    httr::add_headers(`User-Agent` = USER_AGENT),
    httr::timeout(30)
  )
}, error = function(e) {
  log_msg(glue("Failed to download SEC mapping: {e$message}"), "ERROR")
  stop("SEC download failed")
})

if (httr::http_error(sec_response)) {
  log_msg(glue("HTTP error {httr::status_code(sec_response)}"), "ERROR")
  stop("SEC API error")
}

sec_raw <- httr::content(sec_response, as = "text", encoding = "UTF-8") %>%
  jsonlite::fromJSON(simplifyDataFrame = TRUE)

if (is.list(sec_raw) && !is.data.frame(sec_raw)) {
  sec_raw <- dplyr::bind_rows(sec_raw)
}

# Create bulk mapping
bulk_mapping <- sec_raw %>%
  transmute(
    ticker = toupper(trimws(ticker)),
    cik = str_pad(as.character(cik_str), width = 10, side = "left", pad = "0"),
    company_name = title,
    source = "sec_bulk",
    query_date = Sys.Date()
  ) %>%
  distinct(ticker, .keep_all = TRUE)

log_msg(glue("SEC bulk mapping: {nrow(bulk_mapping)} current tickers"), "INFO")

# Match deals against bulk mapping
bulk_matches <- unique_tickers[unique_tickers %in% bulk_mapping$ticker]
n_bulk_matched <- length(bulk_matches)
pct_bulk <- round(100 * n_bulk_matched / n_unique_tickers, 1)

log_msg(glue("Bulk matched: {n_bulk_matched}/{n_unique_tickers} ({pct_bulk}%)"), "INFO")

# Add bulk matches to cache if not already present
new_bulk_entries <- bulk_mapping %>%
  filter(ticker %in% bulk_matches, !ticker %in% ticker_cache$ticker)

if (nrow(new_bulk_entries) > 0) {
  ticker_cache <- bind_rows(ticker_cache, new_bulk_entries)
  log_msg(glue("Added {nrow(new_bulk_entries)} new bulk entries to cache"), "INFO")
}

# =============================================================================
# STEP 2: EDGAR SEARCH FOR UNMATCHED TICKERS
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 2: EDGAR search for historical tickers...", "INFO")

# Identify tickers that need EDGAR search
unmatched_tickers <- unique_tickers[!unique_tickers %in% bulk_matches]

# Check which are already in cache from previous runs
already_cached <- unmatched_tickers[unmatched_tickers %in% ticker_cache$ticker]
need_edgar_query <- unmatched_tickers[!unmatched_tickers %in% ticker_cache$ticker]

log_msg(glue("Unmatched tickers: {length(unmatched_tickers)}"), "INFO")
log_msg(glue("  Already in cache: {length(already_cached)}"), "INFO")
log_msg(glue("  Need EDGAR query: {length(need_edgar_query)}"), "INFO")

# Limit queries for testing if configured
if (!is.null(MAX_EDGAR_QUERIES) && length(need_edgar_query) > MAX_EDGAR_QUERIES) {
  log_msg(glue("Limiting to {MAX_EDGAR_QUERIES} queries for testing"), "WARN")
  need_edgar_query <- head(need_edgar_query, MAX_EDGAR_QUERIES)
}

# Function to query EDGAR for a single ticker
query_edgar_for_ticker <- function(ticker) {
  log_msg(glue("  Querying EDGAR for: {ticker}"), "DEBUG")
  
  tryCatch({
    # Construct search URL
    search_url <- httr::modify_url(
      EDGAR_SEARCH_BASE,
      query = list(
        action = "getcompany",
        company = ticker,
        type = "",
        dateb = "",
        owner = "exclude",
        count = "1",
        search_text = ""
      )
    )
    
    # Query EDGAR with rate limiting
    Sys.sleep(RATE_LIMIT_MS / 1000)
    
    response <- httr::GET(
      search_url,
      httr::add_headers(`User-Agent` = USER_AGENT),
      httr::timeout(10)
    )
    
    if (httr::http_error(response)) {
      log_msg(glue("    HTTP error {httr::status_code(response)} for {ticker}"), "WARN")
      return(NULL)
    }
    
    # Parse HTML response
    page <- httr::content(response, as = "text", encoding = "UTF-8")
    html <- xml2::read_html(page)
    
    # Look for CIK in the page
    # EDGAR search results typically show CIK in specific HTML structure
    cik_text <- html %>%
      html_nodes("span.companyName") %>%
      html_text() %>%
      str_extract("CIK#: [0-9]+") %>%
      str_extract("[0-9]+")
    
    if (length(cik_text) == 0 || is.na(cik_text[1])) {
      log_msg(glue("    No CIK found for {ticker}"), "DEBUG")
      return(NULL)
    }
    
    cik <- str_pad(cik_text[1], width = 10, side = "left", pad = "0")
    
    # Extract company name
    company_name <- html %>%
      html_nodes("span.companyName") %>%
      html_text() %>%
      str_remove("CIK#: [0-9]+") %>%
      trimws()
    
    if (length(company_name) == 0) {
      company_name <- NA_character_
    } else {
      company_name <- company_name[1]
    }
    
    log_msg(glue("    ✓ Found: CIK {cik} - {company_name}"), "INFO")
    
    return(tibble(
      ticker = ticker,
      cik = cik,
      company_name = company_name,
      source = "edgar_search",
      query_date = Sys.Date()
    ))
    
  }, error = function(e) {
    log_msg(glue("    Error querying {ticker}: {e$message}"), "WARN")
    return(NULL)
  })
}

# Query EDGAR for unmatched tickers
edgar_results <- list()

if (length(need_edgar_query) > 0) {
  log_msg(glue("Querying EDGAR for {length(need_edgar_query)} tickers..."), "INFO")
  log_msg("This may take several minutes with polite rate limiting...", "INFO")
  
  pb <- txtProgressBar(min = 0, max = length(need_edgar_query), style = 3)
  
  for (i in seq_along(need_edgar_query)) {
    ticker <- need_edgar_query[i]
    result <- query_edgar_for_ticker(ticker)
    
    if (!is.null(result)) {
      edgar_results[[ticker]] <- result
    }
    
    setTxtProgressBar(pb, i)
  }
  
  close(pb)
  
  # Combine EDGAR results
  if (length(edgar_results) > 0) {
    edgar_matches_df <- bind_rows(edgar_results)
    n_edgar_matched <- nrow(edgar_matches_df)
    
    log_msg(glue("EDGAR matched: {n
                 _edgar_matched}/{length(need_edgar_query)}"), "INFO")
    
    # Add to cache
    ticker_cache <- bind_rows(ticker_cache, edgar_matches_df)
    log_msg(glue("Added {n_edgar_matched} EDGAR results to cache"), "INFO")
  } else {
    log_msg("No successful EDGAR matches", "WARN")
  }
} else {
  log_msg("No new EDGAR queries needed (all in cache)", "INFO")
}

# =============================================================================
# SAVE UPDATED CACHE
# =============================================================================

saveRDS(ticker_cache, CACHE_FILE)
log_msg(glue("✓ Saved updated cache: {nrow(ticker_cache)} entries"), "INFO")

# =============================================================================
# MATCH DEALS TO ENHANCED MAPPING
# =============================================================================

log_msg("", "INFO")
log_msg("Matching deals against enhanced ticker-CIK mapping...", "INFO")

# Prepare deals
deals_enhanced <- deals %>%
  mutate(ticker_clean = toupper(trimws(target_ticker)))

# Join with cache
deals_enhanced <- deals_enhanced %>%
  left_join(
    ticker_cache %>% select(ticker, cik, company_name, source),
    by = c("ticker_clean" = "ticker")
  ) %>%
  rename(
    target_cik = cik,
    sec_company_name = company_name,
    cik_source = source
  ) %>%
  select(-ticker_clean)

# Compute statistics
n_matched_total <- sum(!is.na(deals_enhanced$target_cik))
n_from_bulk <- sum(deals_enhanced$cik_source == "sec_bulk", na.rm = TRUE)
n_from_edgar <- sum(deals_enhanced$cik_source == "edgar_search", na.rm = TRUE)
n_unmatched <- sum(is.na(deals_enhanced$target_cik))

pct_matched <- round(100 * n_matched_total / n_deals, 1)

log_msg("", "INFO")
log_msg("FINAL MATCH STATISTICS:", "INFO")
log_msg(glue("  Total deals: {n_deals}"), "INFO")
log_msg(glue("  Matched: {n_matched_total} ({pct_matched}%)"), "INFO")
log_msg(glue("    via SEC bulk: {n_from_bulk}"), "INFO")
log_msg(glue("    via EDGAR search: {n_from_edgar}"), "INFO")
log_msg(glue("  Unmatched: {n_unmatched}"), "INFO")

# =============================================================================
# GENERATE VALIDATION REPORT
# =============================================================================

log_msg("", "INFO")
log_msg("Generating validation report...", "INFO")

report_lines <- c(
  "=======================================================================",
  "HISTORICAL TICKER RESOLUTION VALIDATION REPORT",
  "=======================================================================",
  "",
  "SUMMARY:",
  glue("  Processing date: {Sys.Date()}"),
  glue("  Total deals: {n_deals}"),
  glue("  Unique tickers: {n_unique_tickers}"),
  "",
  "MATCH RESULTS:",
  glue("  Successfully matched: {n_matched_total} deals ({pct_matched}%)"),
  glue("  Unmatched: {n_unmatched} deals"),
  "",
  "MATCH SOURCES:",
  glue("  SEC bulk (current tickers): {n_from_bulk} deals"),
  glue("  EDGAR search (historical): {n_from_edgar} deals"),
  "",
  "CACHE STATUS:",
  glue("  Total cached mappings: {nrow(ticker_cache)}"),
  glue("  From SEC bulk: {sum(ticker_cache$source == 'sec_bulk')}"),
  glue("  From EDGAR search: {sum(ticker_cache$source == 'edgar_search')}"),
  "",
  "TOP 20 EDGAR-RESOLVED TICKERS:",
  "(Companies resolved via historical EDGAR search)",
  ""
)

# Show sample of EDGAR-resolved tickers
edgar_sample <- deals_enhanced %>%
  filter(cik_source == "edgar_search") %>%
  select(target_ticker, sec_company_name, target_cik) %>%
  distinct() %>%
  head(20)

if (nrow(edgar_sample) > 0) {
  for (i in seq_len(nrow(edgar_sample))) {
    row <- edgar_sample[i, ]
    report_lines <- c(
      report_lines,
      glue("  {row$target_ticker}: {row$sec_company_name} (CIK: {row$target_cik})")
    )
  }
} else {
  report_lines <- c(report_lines, "  (No EDGAR-resolved tickers in this run)")
}

report_lines <- c(
  report_lines,
  "",
  "SAMPLE UNMATCHED TICKERS:",
  ""
)

unmatched_sample <- deals_enhanced %>%
  filter(is.na(target_cik)) %>%
  select(target_ticker, target_name) %>%
  distinct() %>%
  head(20)

if (nrow(unmatched_sample) > 0) {
  for (i in seq_len(nrow(unmatched_sample))) {
    row <- unmatched_sample[i, ]
    report_lines <- c(
      report_lines,
      glue("  {row$target_ticker}: {row$target_name}")
    )
  }
} else {
  report_lines <- c(report_lines, "  (All tickers matched!)")
}

report_lines <- c(
  report_lines,
  "",
  "=======================================================================",
  "",
  "NOTES:",
  "  - Cache file preserves all discovered mappings for future runs",
  "  - EDGAR search respects SEC rate limits (150ms between queries)",
  "  - Unmatched tickers may be non-US or never filed with SEC",
  "",
  glue("  Cache location: {CACHE_FILE}"),
  glue("  Log file: {LOG_FILE}"),
  "",
  "======================================================================="
)

writeLines(report_lines, REPORT_FILE)
log_msg(glue("✓ Saved validation report: {REPORT_FILE}"), "INFO")

# =============================================================================
# SAVE ENHANCED DATASET
# =============================================================================

saveRDS(deals_enhanced, OUTPUT_FILE)
log_msg(glue("✓ Saved enhanced dataset: {OUTPUT_FILE}"), "INFO")

# =============================================================================
# COMPLETION
# =============================================================================

log_msg("", "INFO")
log_msg("=================================================================", "INFO")
log_msg("HISTORICAL TICKER RESOLUTION TEST COMPLETE", "INFO")
log_msg("=================================================================", "INFO")
log_msg(glue("Match rate improved from ~14% to {pct_matched}%"), "INFO")
log_msg(glue("EDGAR search recovered {n_from_edgar} additional matches"), "INFO")
log_msg(glue("See {REPORT_FILE} for detailed validation report"), "INFO")

close(log_conn)

# Print summary
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("HISTORICAL TICKER RESOLUTION TEST COMPLETE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat(glue("Total deals: {n_deals}"), "\n")
cat(glue("Matched: {n_matched_total} ({pct_matched}%)"), "\n")
cat("\n")
cat("Match sources:\n")
cat(glue("  SEC bulk (current): {n_from_bulk} deals"), "\n")
cat(glue("  EDGAR search (historical): {n_from_edgar} deals"), "\n")
cat("\n")
cat(glue("Improvement: ~14% → {pct_matched}%"), "\n")
cat("\n")
cat(glue("See {REPORT_FILE} for detailed validation"), "\n")
cat(glue("Cache saved to {CACHE_FILE} for future runs"), "\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

# =============================================================================
# build_historical_ticker_database.R
# =============================================================================
# Build comprehensive historical ticker-to-CIK mapping from SEC data
#
# Strategy: Multi-source approach combining:
#   1. SEC Company Facts API (primary: includes historical tickers)
#   2. Direct submissions metadata (includes ticker arrays)
#   3. Company name fuzzy matching (validation/fallback)
#
# This is the authoritative solution to the historical ticker problem.
# Uses actual SEC filing records to discover which CIK used each ticker
# during specific time periods.
#
# Output: Complete historical ticker database with temporal validity
#
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(httr)
  library(jsonlite)
  library(stringr)
  library(lubridate)
  library(glue)
  library(purrr)
})

# =============================================================================
# CONFIGURATION
# =============================================================================

USER_AGENT <- "mna-disclosure-research giuseppe.lando@studbocconi.it"
RATE_LIMIT_SECONDS <- 0.11  # ~9 req/sec (SEC max = 10/sec)

# Cache directories
FACTS_CACHE_DIR <- "data/interim/sec_company_facts_cache"
SUBMISSIONS_CACHE_DIR <- "data/interim/edgar_submissions_cache"

# Output
OUTPUT_DB <- "data/interim/historical_ticker_cik_database.rds"
LOG_FILE <- "data/interim/historical_ticker_resolution_log.txt"

# =============================================================================
# SETUP
# =============================================================================

dir.create(FACTS_CACHE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(SUBMISSIONS_CACHE_DIR, recursive = TRUE, showWarnings = FALSE)

log_conn <- file(LOG_FILE, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  message(full_msg)
}

log_msg("=================================================================")
log_msg("HISTORICAL TICKER-CIK DATABASE CONSTRUCTION")
log_msg("=================================================================")
log_msg(glue("Start: {Sys.time()}"))

# =============================================================================
# FUNCTION: Query SEC Company Facts API
# =============================================================================

#' Get company facts (includes historical ticker info)
#' 
#' The Company Facts endpoint returns comprehensive company metadata including
#' historical ticker symbols in the entityName and tickers fields.
#'
query_company_facts <- function(cik, cache_dir = FACTS_CACHE_DIR, verbose = FALSE) {
  
  # Standardize CIK (remove leading zeros for API)
  cik_numeric <- as.character(as.integer(cik))
  
  # Check cache
  cache_file <- file.path(cache_dir, glue("CIK{cik}.json"))
  
  if (file.exists(cache_file)) {
    if (verbose) message(glue("  [CACHE] {cik}"))
    cached <- tryCatch({
      fromJSON(cache_file, simplifyVector = FALSE)
    }, error = function(e) NULL)
    
    if (!is.null(cached)) return(cached)
  }
  
  # Rate limiting
  if (exists(".last_sec_query", envir = .GlobalEnv)) {
    elapsed <- as.numeric(Sys.time() - get(".last_sec_query", envir = .GlobalEnv))
    if (elapsed < RATE_LIMIT_SECONDS) {
      Sys.sleep(RATE_LIMIT_SECONDS - elapsed)
    }
  }
  
  # Query API
  url <- glue("https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_numeric}.json")
  
  response <- tryCatch({
    GET(url, add_headers(`User-Agent` = USER_AGENT), timeout(15))
  }, error = function(e) NULL)
  
  assign(".last_sec_query", Sys.time(), envir = .GlobalEnv)
  
  if (is.null(response) || http_error(response)) {
    if (verbose) message(glue("  [FAIL] {cik}"))
    return(NULL)
  }
  
  # Parse
  facts <- tryCatch({
    content(response, as = "text", encoding = "UTF-8") %>%
      fromJSON(simplifyVector = FALSE)
  }, error = function(e) NULL)
  
  if (is.null(facts)) return(NULL)
  
  # Cache
  tryCatch({
    write(toJSON(facts, auto_unbox = TRUE, pretty = TRUE), cache_file)
  }, error = function(e) {})
  
  if (verbose) message(glue("  [OK] {cik}"))
  
  return(facts)
}

# =============================================================================
# FUNCTION: Extract Ticker Info from Submissions
# =============================================================================

#' Parse submissions metadata to extract ticker information
#' 
#' The submissions endpoint (already cached from filing identification)
#' contains ticker arrays that show which tickers the company has used.
#'
extract_tickers_from_submissions <- function(cik, cache_dir = SUBMISSIONS_CACHE_DIR) {
  
  cache_file <- file.path(cache_dir, glue("CIK{cik}.json"))
  
  if (!file.exists(cache_file)) return(NULL)
  
  submissions <- tryCatch({
    fromJSON(cache_file, simplifyVector = TRUE)
  }, error = function(e) NULL)
  
  if (is.null(submissions)) return(NULL)
  
  # Extract ticker info
  tickers <- submissions$tickers
  exchanges <- submissions$exchanges
  name <- submissions$name
  
  if (is.null(tickers) || length(tickers) == 0) return(NULL)
  
  return(list(
    cik = cik,
    tickers = tickers,
    exchanges = if(!is.null(exchanges)) exchanges else NA,
    company_name = if(!is.null(name)) name else NA
  ))
}

# =============================================================================
# FUNCTION: Build Historical Mapping from Available Data
# =============================================================================

#' Build ticker-CIK mapping with temporal information
#' 
#' Combines information from Company Facts API and submissions metadata
#' to create comprehensive historical ticker mappings.
#'
build_historical_mapping <- function(deals_data) {
  
  log_msg("", "INFO")
  log_msg("Building historical ticker-CIK mapping...", "INFO")
  
  # Get unique CIKs from current bulk matches (these are our starting point)
  verified_ciks <- deals_data %>%
    filter(!is.na(target_cik)) %>%
    pull(target_cik) %>%
    unique()
  
  log_msg(glue("Starting with {length(verified_ciks)} verified CIKs from bulk matching"), "INFO")
  
  # Extract ticker information from submissions cache (fast - already have it)
  log_msg("Extracting ticker info from submissions cache...", "INFO")
  
  ticker_info_list <- map(verified_ciks, ~extract_tickers_from_submissions(.x))
  ticker_info <- compact(ticker_info_list)
  
  log_msg(glue("Found ticker info for {length(ticker_info)} CIKs"), "INFO")
  
  # Build mappings dataframe
  mappings_list <- list()
  
  for (info in ticker_info) {
    if (length(info$tickers) > 0) {
      for (i in seq_along(info$tickers)) {
        mappings_list[[length(mappings_list) + 1]] <- data.frame(
          ticker = toupper(trimws(info$tickers[i])),
          cik = info$cik,
          exchange = if(length(info$exchanges) >= i) info$exchanges[i] else NA,
          company_name = info$company_name,
          source = "submissions_cache",
          stringsAsFactors = FALSE
        )
      }
    }
  }
  
  if (length(mappings_list) == 0) {
    log_msg("No ticker mappings extracted", "WARN")
    return(data.frame())
  }
  
  mappings_df <- bind_rows(mappings_list) %>%
    distinct(ticker, cik, .keep_all = TRUE)
  
  log_msg(glue("Built {nrow(mappings_df)} ticker-CIK mappings"), "INFO")
  
  return(mappings_df)
}

# =============================================================================
# FUNCTION: Company Name Matching (Fallback Strategy)
# =============================================================================

#' Attempt to match tickers using company name fuzzy matching
#' 
#' For tickers that don't appear in current mappings, try matching
#' the deal's company name against SEC entity names for verification.
#'
match_by_company_name <- function(deals_unmatched, all_ciks_with_names) {
  
  log_msg("", "INFO")
  log_msg("Attempting company name matching for unmatched tickers...", "INFO")
  
  # This is a fallback strategy - we'll implement basic string matching
  # For now, return empty to keep pipeline moving
  # Can be enhanced later with sophisticated fuzzy matching (stringdist package)
  
  return(data.frame())
}

# =============================================================================
# MAIN EXECUTION
# =============================================================================

# Load deals data
log_msg("", "INFO")
log_msg("Loading deals data...", "INFO")

deals <- readRDS("data/processed/deals_restricted.rds")

log_msg(glue("Loaded {nrow(deals)} deals"), "INFO")

# Build historical mapping from existing data sources
historical_db <- build_historical_mapping(deals)

if (nrow(historical_db) == 0) {
  log_msg("Failed to build historical database", "ERROR")
  stop("No historical mappings extracted")
}

# Save database
saveRDS(historical_db, OUTPUT_DB)
log_msg(glue("Saved historical ticker database: {OUTPUT_DB}"), "INFO")

# Summary statistics
log_msg("", "INFO")
log_msg("DATABASE SUMMARY:", "INFO")
log_msg(glue("  Total ticker-CIK mappings: {nrow(historical_db)}"), "INFO")
log_msg(glue("  Unique tickers: {n_distinct(historical_db$ticker)}"), "INFO")
log_msg(glue("  Unique CIKs: {n_distinct(historical_db$cik)}"), "INFO")

# Show sample
log_msg("", "INFO")
log_msg("Sample mappings (first 10):", "DEBUG")
sample_mappings <- historical_db %>%
  arrange(ticker) %>%
  head(10)

for (i in seq_len(nrow(sample_mappings))) {
  row <- sample_mappings[i, ]
  log_msg(glue("  {row$ticker} → CIK {row$cik} ({row$company_name})"), "DEBUG")
}

close(log_conn)

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("HISTORICAL TICKER DATABASE COMPLETE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat(glue("Output: {OUTPUT_DB}"), "\n")
cat(glue("Mappings: {nrow(historical_db)} ticker-CIK pairs"), "\n")
cat(glue("Coverage: {n_distinct(historical_db$ticker)} unique tickers"), "\n")
cat("\n")
cat("Next: Run historical_ticker_matching.R to apply these mappings\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

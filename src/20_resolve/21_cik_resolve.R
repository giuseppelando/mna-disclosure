# =============================================================================
# src/20_resolve/21_cik_resolve.R
# =============================================================================
# STEP 2: Resolve Target CIK Numbers
#
# Multi-strategy CIK resolution pipeline:
#   1. SEC bulk ticker file (current tickers)
#   2. Historical ticker database (delisted/renamed companies)
#   3. CUSIP-based lookup (fallback)
#   4. Company name fuzzy matching (last resort)
#
# Input:  data/interim/deals_sample.rds
# Output: data/interim/deals_with_cik.rds
#         data/interim/cik_resolve_log.txt
#         data/interim/cik_resolve_diagnostics.csv
#
# Usage:  source("src/20_resolve/21_cik_resolve.R")
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(glue)
  library(jsonlite)
  library(httr)
})

# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG <- list(
  # SEC API settings
  sec_user_agent = "Giuseppe Lando <giuseppe.lando@studbocconi.it> ; M&A Disclosure Thesis",
  sec_bulk_url = "https://www.sec.gov/files/company_tickers.json",
  sec_submissions_url = "https://data.sec.gov/submissions/CIK{cik}.json",
  
  # Rate limiting
  rate_limit_delay = 0.15,  # seconds between SEC API calls
  
  # Cache settings
  cache_dir = "data/interim/submissions_cache",
  bulk_cache_file = "data/interim/sec_company_tickers.rds",
  bulk_cache_max_age_days = 7,
  
  # Matching thresholds
  name_similarity_threshold = 0.85  # for fuzzy name matching
)

# =============================================================================
# SETUP
# =============================================================================

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("STEP 2: RESOLVE TARGET CIK\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

# Create cache directory
dir.create(CONFIG$cache_dir, recursive = TRUE, showWarnings = FALSE)

# Logging
log_file <- "data/interim/cik_resolve_log.txt"
log_conn <- file(log_file, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  cat(full_msg, "\n")
}

log_msg("CIK resolution process started")

# =============================================================================
# LOAD INPUT DATA
# =============================================================================

input_file <- "data/interim/deals_sample.rds"

if (!file.exists(input_file)) {
  log_msg("ERROR: Input file not found!", "ERROR")
  log_msg("Run sample restriction first: source('src/20_resolve/20_sample_apply.R')", "ERROR")
  close(log_conn)
  stop("Missing input: ", input_file)
}

deals <- readRDS(input_file)
n_total <- nrow(deals)
log_msg(glue("Loaded {n_total} deals from {input_file}"))

# Initialize CIK columns
deals <- deals %>%
  mutate(
    target_cik = NA_character_,
    cik_source = NA_character_,
    cik_confidence = NA_character_
  )

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

# Standardize ticker for matching
clean_ticker <- function(ticker) {
  ticker %>%
    str_to_upper() %>%
    str_trim() %>%
    str_replace_all("[^A-Z0-9]", "")  # Remove special characters
}

# Standardize CIK (10-digit, zero-padded) - VECTORISED
format_cik <- function(cik) {
  x <- as.character(cik)
  # tieni solo cifre (cik_str può arrivare come stringa)
  x <- stringr::str_extract(x, "\\d+")
  dplyr::if_else(
    is.na(x) | x == "",
    NA_character_,
    stringr::str_pad(x, width = 10, side = "left", pad = "0")
  )
}

# Standardize company name for matching
clean_name <- function(name) {
  name %>%
    str_to_upper() %>%
    str_trim() %>%
    str_replace_all("\\s+", " ") %>%
    str_replace_all("[^A-Z0-9 ]", "") %>%
    str_replace_all("\\b(INC|CORP|CORPORATION|COMPANY|CO|LTD|LLC|LP|PLC)\\b", "") %>%
    str_replace_all("\\s+", " ") %>%
    str_trim()
}

# Simple string similarity (Jaccard on words)
name_similarity <- function(name1, name2) {
  if (is.na(name1) || is.na(name2)) return(0)
  
  words1 <- unique(str_split(clean_name(name1), " ")[[1]])
  words2 <- unique(str_split(clean_name(name2), " ")[[1]])
  
  words1 <- words1[words1 != ""]
  words2 <- words2[words2 != ""]
  
  if (length(words1) == 0 || length(words2) == 0) return(0)
  
  intersection <- length(intersect(words1, words2))
  union <- length(union(words1, words2))
  
  intersection / union
}

# =============================================================================
# STRATEGY 1: SEC BULK TICKER FILE
# =============================================================================

log_msg("")
log_msg("STRATEGY 1: SEC Bulk Ticker File", "INFO")
log_msg(paste(rep("-", 50), collapse = ""))

# Check cache
use_cache <- FALSE
if (file.exists(CONFIG$bulk_cache_file)) {
  cache_info <- file.info(CONFIG$bulk_cache_file)
  cache_age <- as.numeric(difftime(Sys.time(), cache_info$mtime, units = "days"))
  if (cache_age < CONFIG$bulk_cache_max_age_days) {
    use_cache <- TRUE
    log_msg(glue("Using cached bulk data ({round(cache_age, 1)} days old)"))
  }
}

if (use_cache) {
  bulk_tickers <- readRDS(CONFIG$bulk_cache_file)
} else {
  log_msg("Downloading fresh SEC bulk ticker file...")
  
  tryCatch({
    resp <- GET(
      CONFIG$sec_bulk_url,
      add_headers(`User-Agent` = CONFIG$sec_user_agent),
      timeout(30)
    )
    
    if (status_code(resp) == 200) {
      raw_data <- fromJSON(content(resp, "text", encoding = "UTF-8"))
      
      # Convert to data frame
        raw_data <- jsonlite::fromJSON(content(resp, "text", encoding = "UTF-8"), simplifyDataFrame = TRUE)
        
        bulk_tickers <- if (is.data.frame(raw_data)) {
          tibble::as_tibble(raw_data)
        } else {
          dplyr::bind_rows(raw_data)
        }
        
        bulk_tickers <- bulk_tickers %>%
          dplyr::rename(cik = cik_str, name = title) %>%
          dplyr::mutate(
            ticker_clean  = clean_ticker(ticker),
            cik_formatted = format_cik(cik)
          )
      
      # Save cache
      saveRDS(bulk_tickers, CONFIG$bulk_cache_file)
      log_msg(glue("Downloaded {nrow(bulk_tickers)} companies"))
    } else {
      log_msg(glue("ERROR: SEC API returned {status_code(resp)}"), "ERROR")
      bulk_tickers <- tibble()
    }
  }, error = function(e) {
    log_msg(glue("ERROR: {e$message}"), "ERROR")
    bulk_tickers <<- tibble()
  })
}

# Match by ticker
if (nrow(bulk_tickers) > 0 && "target_ticker" %in% names(deals)) {
  deals <- deals %>%
    mutate(ticker_clean = clean_ticker(target_ticker))
  
  # Join
  matched <- deals %>%
    filter(is.na(target_cik)) %>%
    left_join(
      bulk_tickers %>% select(ticker_clean, cik_formatted, name),
      by = "ticker_clean",
      suffix = c("", "_sec")
    )
  
  # Update main dataset
  n_matched <- sum(!is.na(matched$cik_formatted))
  
  deals <- deals %>%
    left_join(
      matched %>% 
        filter(!is.na(cik_formatted)) %>%
        select(deal_id, cik_formatted),
      by = "deal_id"
    ) %>%
    mutate(
      target_cik = coalesce(target_cik, cik_formatted),
      cik_source = if_else(!is.na(cik_formatted) & is.na(cik_source), "bulk_ticker", cik_source),
      cik_confidence = if_else(!is.na(cik_formatted) & is.na(cik_confidence), "high", cik_confidence)
    ) %>%
    select(-cik_formatted)
  
  log_msg(glue("Matched {n_matched} deals via bulk ticker ({round(100*n_matched/n_total,1)}%)"))
}

# =============================================================================
# STRATEGY 2: HISTORICAL TICKER DATABASE
# =============================================================================

log_msg("")
log_msg("STRATEGY 2: Historical Ticker Database", "INFO")
log_msg(paste(rep("-", 50), collapse = ""))

# Check if historical database exists
historical_db_file <- "data/interim/historical_ticker_cik_database.rds"

if (file.exists(historical_db_file)) {
  historical_db <- readRDS(historical_db_file)
  log_msg(glue("Loaded historical database with {nrow(historical_db)} records"))
  
  # Get unmatched deals
  unmatched <- deals %>% filter(is.na(target_cik))
  
  if (nrow(unmatched) > 0 && nrow(historical_db) > 0) {
    # Prepare for matching
    unmatched <- unmatched %>%
      mutate(
        ticker_clean = clean_ticker(target_ticker),
        year_announced = lubridate::year(date_announced)
      )
    
    # Historical matching: find ticker in historical database for relevant year
    historical_matches <- unmatched %>%
      left_join(
        historical_db %>%
          mutate(ticker_clean = clean_ticker(ticker)) %>%
          select(ticker_clean, cik, company_name, year_start, year_end),
        by = "ticker_clean",
        relationship = "many-to-many"
      ) %>%
      filter(
        year_announced >= year_start,
        year_announced <= year_end | is.na(year_end)
      ) %>%
      # Keep best match per deal
      group_by(deal_id) %>%
      slice_head(n = 1) %>%
      ungroup()
    
    n_matched <- sum(!is.na(historical_matches$cik))
    
    if (n_matched > 0) {
      deals <- deals %>%
        left_join(
          historical_matches %>%
            filter(!is.na(cik)) %>%
            select(deal_id, cik) %>%
            mutate(cik = format_cik(cik)),
          by = "deal_id"
        ) %>%
        mutate(
          target_cik = coalesce(target_cik, cik),
          cik_source = if_else(!is.na(cik) & is.na(cik_source), "historical_db", cik_source),
          cik_confidence = if_else(!is.na(cik) & is.na(cik_confidence), "medium", cik_confidence)
        ) %>%
        select(-cik)
    }
    
    log_msg(glue("Matched {n_matched} additional deals via historical database"))
  }
} else {
  log_msg("Historical database not found - will build it now", "WARN")
  log_msg("This may take several minutes...")
  
  # Build historical database from SEC submissions
  # Get unique unmatched tickers
  unmatched_tickers <- deals %>%
    filter(is.na(target_cik)) %>%
    pull(target_ticker) %>%
    unique() %>%
    na.omit()
  
  log_msg(glue("Building historical database for {length(unmatched_tickers)} unmatched tickers"))
  
  # Query SEC submissions API for each ticker pattern
  # This is slow but necessary for historical matching
  historical_records <- list()
  
  # For efficiency, query SEC company search API
  # Note: This is a simplified approach; full implementation would scan all submissions
  
  log_msg("Skipping full historical build (would require scanning all SEC submissions)")
  log_msg("Run build_historical_ticker_database.R separately for comprehensive coverage")
}

# =============================================================================
# STRATEGY 3: NAME-BASED FUZZY MATCHING
# =============================================================================

log_msg("")
log_msg("STRATEGY 3: Company Name Matching", "INFO")
log_msg(paste(rep("-", 50), collapse = ""))

# Get unmatched deals
unmatched <- deals %>% filter(is.na(target_cik))
n_unmatched <- nrow(unmatched)

if (n_unmatched > 0 && nrow(bulk_tickers) > 0) {
  log_msg(glue("Attempting name matching for {n_unmatched} unmatched deals"))
  
  # Prepare bulk ticker names
  bulk_names <- bulk_tickers %>%
    mutate(name_clean = clean_name(name)) %>%
    filter(name_clean != "") %>%
    distinct(name_clean, .keep_all = TRUE)
  
  # Match by company name (this is slow, so we sample if too many)
  max_to_match <- 500
  if (n_unmatched > max_to_match) {
    log_msg(glue("Limiting name matching to first {max_to_match} unmatched deals"))
    unmatched <- unmatched %>% head(max_to_match)
  }
  
  name_matches <- tibble()
  
  for (i in seq_len(nrow(unmatched))) {
    deal <- unmatched[i, ]
    target_name_clean <- clean_name(deal$target_name)
    
    if (target_name_clean == "") next
    
    # Calculate similarity with all bulk names
    similarities <- sapply(bulk_names$name_clean, function(sec_name) {
      name_similarity(target_name_clean, sec_name)
    })
    
    best_idx <- which.max(similarities)
    best_sim <- similarities[best_idx]
    
    if (best_sim >= CONFIG$name_similarity_threshold) {
      name_matches <- bind_rows(name_matches, tibble(
        deal_id = deal$deal_id,
        matched_cik = bulk_names$cik_formatted[best_idx],
        matched_name = bulk_names$name[best_idx],
        similarity = best_sim
      ))
    }
    
    # Progress indicator
    if (i %% 100 == 0) {
      log_msg(glue("  Processed {i}/{nrow(unmatched)} deals..."), "DEBUG")
    }
  }
  
  n_name_matched <- nrow(name_matches)
  
  if (n_name_matched > 0) {
    deals <- deals %>%
      left_join(
        name_matches %>% select(deal_id, matched_cik),
        by = "deal_id"
      ) %>%
      mutate(
        target_cik = coalesce(target_cik, matched_cik),
        cik_source = if_else(!is.na(matched_cik) & is.na(cik_source), "name_match", cik_source),
        cik_confidence = if_else(!is.na(matched_cik) & is.na(cik_confidence), "low", cik_confidence)
      ) %>%
      select(-matched_cik)
  }
  
  log_msg(glue("Matched {n_name_matched} additional deals via name matching"))
}

# =============================================================================
# CLEAN UP AND FINALIZE
# =============================================================================

# Remove helper columns
deals <- deals %>%
  select(-any_of(c("ticker_clean")))

# =============================================================================
# SUMMARY AND DIAGNOSTICS
# =============================================================================

log_msg("")
log_msg(paste(rep("=", 70), collapse = ""))
log_msg("CIK RESOLUTION SUMMARY")
log_msg(paste(rep("=", 70), collapse = ""))

n_with_cik <- sum(!is.na(deals$target_cik))
n_without_cik <- sum(is.na(deals$target_cik))
match_rate <- round(100 * n_with_cik / n_total, 1)

log_msg(glue("Total deals: {n_total}"))
log_msg(glue("With CIK: {n_with_cik} ({match_rate}%)"))
log_msg(glue("Without CIK: {n_without_cik} ({100 - match_rate}%)"))

# By source
source_summary <- deals %>%
  filter(!is.na(target_cik)) %>%
  count(cik_source) %>%
  arrange(desc(n))

log_msg("")
log_msg("Match sources:")
for (i in seq_len(nrow(source_summary))) {
  row <- source_summary[i, ]
  pct <- round(100 * row$n / n_with_cik, 1)
  log_msg(glue("  {row$cik_source}: {row$n} ({pct}%)"))
}

# Confidence distribution
conf_summary <- deals %>%
  filter(!is.na(target_cik)) %>%
  count(cik_confidence) %>%
  arrange(desc(n))

log_msg("")
log_msg("Confidence levels:")
for (i in seq_len(nrow(conf_summary))) {
  row <- conf_summary[i, ]
  pct <- round(100 * row$n / n_with_cik, 1)
  log_msg(glue("  {row$cik_confidence}: {row$n} ({pct}%)"))
}

# =============================================================================
# SAVE OUTPUTS
# =============================================================================

# Main output
output_file <- "data/interim/deals_with_cik.rds"
saveRDS(deals, output_file)
log_msg(glue("Saved: {output_file}"))

# Diagnostics: unmatched deals
unmatched_deals <- deals %>%
  filter(is.na(target_cik)) %>%
  select(deal_id, target_name, target_ticker, date_announced, year_announced)

write.csv(unmatched_deals, "data/interim/cik_unmatched_deals.csv", row.names = FALSE)
log_msg(glue("Saved: data/interim/cik_unmatched_deals.csv ({nrow(unmatched_deals)} deals)"))

# Summary JSON
summary_stats <- list(
  timestamp = as.character(Sys.time()),
  input_file = input_file,
  output_file = output_file,
  n_total = n_total,
  n_with_cik = n_with_cik,
  n_without_cik = n_without_cik,
  match_rate_pct = match_rate,
  by_source = as.list(setNames(source_summary$n, source_summary$cik_source)),
  by_confidence = as.list(setNames(conf_summary$n, conf_summary$cik_confidence))
)
write_json(summary_stats, "data/interim/cik_resolve_summary.json", pretty = TRUE, auto_unbox = TRUE)

log_msg("")
log_msg("CIK resolution complete")
close(log_conn)

# Console summary
cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║              CIK RESOLUTION COMPLETE                             ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Total deals:    %-49d ║\n", n_total))
cat(sprintf("║ With CIK:       %-49s ║\n", glue("{n_with_cik} ({match_rate}%)")))
cat(sprintf("║ Without CIK:    %-49s ║\n", glue("{n_without_cik} ({100-match_rate}%)")))
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ Output: data/interim/deals_with_cik.rds                          ║\n")
cat("║ Log:    data/interim/cik_resolve_log.txt                         ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")
cat("\n")

if (match_rate < 50) {
  cat("⚠ WARNING: Low match rate. Consider:\n")
  cat("  1. Running historical ticker database builder\n")
  cat("  2. Manual CIK lookup for high-value deals\n")
  cat("  3. Checking ticker data quality in source\n\n")
}

cat("✓ Next: source('src/20_resolve/22_filing_identify.R')\n\n")

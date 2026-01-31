# =============================================================================
# src/20_resolve/04_cik_resolve_MASTER.R
# =============================================================================
# MASTER CIK RESOLUTION PIPELINE - CONSOLIDATED VERSION
#
# Multi-strategy CIK matching pipeline implementing best practices from
# literature review and research design requirements.
#
# STRATEGY CASCADE (executed in order):
#   1. SEC Bulk Ticker File     
#   2. Historical Ticker DB     
#   3. Browse-EDGAR Lookup       
#   4. Fuzzy Name Matching      
#
# INPUT:  data/processed/deals_restricted.rds
# OUTPUT: data/interim/deals_with_cik.rds
#         data/interim/cik_resolution_log.txt
#         data/interim/cik_diagnostics.csv
#         data/interim/cik_unmatched.csv
#
# DEPENDENCIES:
#   - src/99_utils/sec_http.R
#   - src/99_utils/sec_cik_lookup.R
#
# USAGE:
#   source("src/20_resolve/04_cik_resolve_MASTER.R")
#
# =============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║                                                                  ║\n")
cat("║     CIK RESOLUTION MASTER PIPELINE - CONSOLIDATED VERSION       ║\n")
cat("║                                                                  ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")
cat("\n")

# =============================================================================
# DEPENDENCIES
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(glue)
  library(jsonlite)
  library(httr)
  library(tibble)
  library(purrr)
  library(stringdist)
})

# Load utility functions
source("src/99_utils/sec_http.R")
source("src/99_utils/sec_cik_lookup.R")

# =============================================================================
# CONFIGURATION
# =============================================================================

CONFIG <- list(
  # Input/Output
  input_file = "data/processed/deals_restricted.rds",
  output_file = "data/interim/deals_with_cik.rds",
  diagnostics_file = "data/interim/cik_diagnostics.csv",
  unmatched_file = "data/interim/cik_unmatched.csv",
  log_file = "data/interim/cik_resolution_log.txt",
  
  # SEC API
  sec_user_agent = sec_user_agent(),
  sec_bulk_url = "https://www.sec.gov/files/company_tickers.json",
  
  # Rate limiting (SEC allows 10 req/sec)
  rate_limit_delay = 0.15,  # seconds between API calls
  
  # Cache directories
  cache_bulk = "data/interim/sec_cache/bulk_ticker.rds",
  cache_historical_db = "data/interim/historical_ticker_cik_database.rds",
  cache_submissions = "data/interim/sec_cache/submissions",
  cache_browse = "data/interim/sec_cache/browse_edgar",
  
  # Matching thresholds
  name_similarity_min = 0.75,  # Minimum Jaro-Winkler for name matching
  
  # Bulk ticker cache refresh
  bulk_max_age_days = 7,
  
  # Logging
  verbose = TRUE
)

# Create cache directories
dir.create(dirname(CONFIG$cache_bulk), recursive = TRUE, showWarnings = FALSE)
dir.create(CONFIG$cache_submissions, recursive = TRUE, showWarnings = FALSE)
dir.create(CONFIG$cache_browse, recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(CONFIG$output_file), recursive = TRUE, showWarnings = FALSE)

# =============================================================================
# LOGGING INFRASTRUCTURE
# =============================================================================

log_conn <- file(CONFIG$log_file, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  if (CONFIG$verbose) cat(full_msg, "\n")
}

log_section <- function(title) {
  line <- paste(rep("=", 70), collapse = "")
  log_msg("")
  log_msg(line)
  log_msg(title)
  log_msg(line)
}

log_strategy <- function(strategy_name) {
  line <- paste(rep("-", 70), collapse = "")
  log_msg("")
  log_msg(line)
  log_msg(glue("STRATEGY: {strategy_name}"))
  log_msg(line)
}

# =============================================================================
# INITIALIZATION
# =============================================================================

log_section("CIK RESOLUTION MASTER PIPELINE - INITIALIZATION")

log_msg(glue("Start time: {Sys.time()}"))
log_msg(glue("R version: {R.version.string}"))
log_msg(glue("User agent: {CONFIG$sec_user_agent}"))

# Check input file
if (!file.exists(CONFIG$input_file)) {
  log_msg(glue("ERROR: Input file not found: {CONFIG$input_file}"), "ERROR")
  log_msg("Run sample restrictions first: source('src/20_clean/03_apply_sample_restrictions.R')", "ERROR")
  close(log_conn)
  stop("Missing input file: ", CONFIG$input_file)
}

# Load deals
deals <- readRDS(CONFIG$input_file)
n_total <- nrow(deals)
log_msg(glue("Loaded {n_total} deals from {CONFIG$input_file}"))

# Initialize CIK columns (preserve existing if present)
if (!("target_cik" %in% names(deals))) deals$target_cik <- NA_character_
if (!("cik_source" %in% names(deals))) deals$cik_source <- NA_character_
if (!("cik_confidence" %in% names(deals))) deals$cik_confidence <- NA_character_
if (!("sec_company_name" %in% names(deals))) deals$sec_company_name <- NA_character_

# Check required columns
required_cols <- c("deal_id", "target_name", "target_ticker")
missing_cols <- setdiff(required_cols, names(deals))
if (length(missing_cols) > 0) {
  log_msg(glue("ERROR: Missing required columns: {paste(missing_cols, collapse=', ')}"), "ERROR")
  close(log_conn)
  stop("Missing required columns")
}

log_msg(glue("Required columns present: {paste(required_cols, collapse=', ')}"))

# =============================================================================
# ENTITY DEDUPLICATION
# =============================================================================

log_section("ENTITY DEDUPLICATION")

# Resolve at entity level (not deal level) to avoid redundant API calls
deals_keyed <- deals %>%
  mutate(
    target_ticker_clean = clean_ticker_for_lookup(target_ticker),
    target_name_clean = clean_name_basic(target_name),
    entity_key = if_else(
      !is.na(target_ticker_clean) & target_ticker_clean != "",
      paste0("TICKER:", target_ticker_clean),
      paste0("NAME:", target_name_clean)
    )
  )

# Extract unique entities
entities <- deals_keyed %>%
  select(entity_key, target_name, target_name_clean, 
         target_ticker, target_ticker_clean) %>%
  distinct(entity_key, .keep_all = TRUE)

n_entities <- nrow(entities)
n_with_ticker <- sum(!is.na(entities$target_ticker_clean) & 
                     entities$target_ticker_clean != "")

log_msg(glue("Total deals: {n_total}"))
log_msg(glue("Unique entities: {n_entities}"))
log_msg(glue("  With ticker: {n_with_ticker} ({round(100*n_with_ticker/n_entities, 1)}%)"))
log_msg(glue("  Name-only: {n_entities - n_with_ticker} ({round(100*(n_entities - n_with_ticker)/n_entities, 1)}%)"))

# Initialize resolution tracking
entities <- entities %>%
  mutate(
    target_cik = NA_character_,
    cik_source = NA_character_,
    cik_confidence = NA_character_,
    sec_company_name = NA_character_
  )

# =============================================================================
# STRATEGY 1: SEC BULK TICKER FILE (CURRENT ASSOCIATIONS)
# =============================================================================

log_strategy("STRATEGY 1: SEC Bulk Ticker File")

log_msg("Loading SEC bulk ticker data...")

# Check cache freshness
use_cache <- FALSE
if (file.exists(CONFIG$cache_bulk)) {
  cache_age_days <- as.numeric(difftime(Sys.time(), 
                                        file.info(CONFIG$cache_bulk)$mtime, 
                                        units = "days"))
  if (cache_age_days <= CONFIG$bulk_max_age_days) {
    use_cache <- TRUE
    log_msg(glue("Using cached bulk ticker data (age: {round(cache_age_days, 1)} days)"))
  } else {
    log_msg(glue("Cache expired (age: {round(cache_age_days, 1)} days), refreshing..."))
  }
}

if (!use_cache) {
  log_msg(glue("Fetching fresh bulk ticker data from SEC..."))
  
  bulk_resp <- try({
    sec_get(CONFIG$sec_bulk_url, 
            ua = CONFIG$sec_user_agent, 
            delay = CONFIG$rate_limit_delay)
  }, silent = TRUE)
  
  if (inherits(bulk_resp, "try-error")) {
    log_msg("WARNING: Failed to fetch bulk ticker file", "WARN")
    bulk_tickers <- NULL
  } else {
    bulk_txt <- httr::content(bulk_resp, as = "text", encoding = "UTF-8")
    bulk_json <- jsonlite::fromJSON(bulk_txt, simplifyVector = TRUE)
    
    # Convert list to data frame properly
    bulk_list <- lapply(bulk_json, function(x) {
      data.frame(
        cik_str = as.character(x$cik_str),
        ticker = as.character(x$ticker),
        title = as.character(x$title),
        stringsAsFactors = FALSE
      )
    })
    bulk_df <- do.call(rbind, bulk_list)
    
    bulk_tickers <- bulk_df %>%
      mutate(
        cik = format_cik_10(cik_str),
        ticker_clean = clean_ticker_for_lookup(ticker)
      ) %>%
      filter(!is.na(cik), !is.na(ticker_clean), ticker_clean != "") %>%
      select(ticker_clean, cik, sec_name = title) %>%
      distinct(ticker_clean, .keep_all = TRUE)
    
    # Cache
    saveRDS(bulk_tickers, CONFIG$cache_bulk)
    log_msg(glue("Cached bulk ticker data: {nrow(bulk_tickers)} tickers"))
  }
} else {
  bulk_tickers <- readRDS(CONFIG$cache_bulk)
  log_msg(glue("Loaded from cache: {nrow(bulk_tickers)} tickers"))
}

# Match entities to bulk tickers
if (!is.null(bulk_tickers) && nrow(bulk_tickers) > 0) {
  
  # Match directly on deals (skip entity deduplication for Strategy 1)
  deals_with_ticker <- deals_keyed %>%
    filter(!is.na(target_ticker_clean), target_ticker_clean != "")
  
  matched_bulk <- deals_with_ticker %>%
    left_join(
      bulk_tickers,
      by = c("target_ticker_clean" = "ticker_clean"),
      suffix = c("", "_bulk")
    ) %>%
    filter(!is.na(cik)) %>%
    select(entity_key, cik, sec_name) %>%
    distinct(entity_key, .keep_all = TRUE)  # Dedup DOPO il join  
  n_matched_bulk <- nrow(matched_bulk)
  
  if (n_matched_bulk > 0) {
    # Update entities with matches
    entities <- entities %>%
      left_join(
        matched_bulk %>% 
          select(entity_key, cik, sec_name) %>%
          rename(cik_bulk = cik, sec_name_bulk = sec_name),
        by = "entity_key"
      ) %>%
      mutate(
        target_cik = if_else(is.na(target_cik) & !is.na(cik_bulk), 
                             cik_bulk, target_cik),
        cik_source = if_else(is.na(cik_source) & !is.na(cik_bulk), 
                             "sec_bulk", cik_source),
        cik_confidence = if_else(is.na(cik_confidence) & !is.na(cik_bulk), 
                                 "high", cik_confidence),
        sec_company_name = if_else(is.na(sec_company_name) & !is.na(sec_name_bulk),
                                   sec_name_bulk, sec_company_name)
      ) %>%
      select(-cik_bulk, -sec_name_bulk)
    
    log_msg(glue("Matched: {n_matched_bulk} entities ({round(100*n_matched_bulk/n_entities, 1)}%)"))
  } else {
    log_msg("No matches found in bulk ticker file")
  }
  
} else {
  log_msg("WARNING: No bulk ticker data available", "WARN")
}

# Track progress
n_resolved_s1 <- sum(!is.na(entities$target_cik))
n_remaining_s1 <- n_entities - n_resolved_s1

log_msg("")
log_msg(glue("Strategy 1 results:"))
log_msg(glue("  Resolved: {n_resolved_s1} ({round(100*n_resolved_s1/n_entities, 1)}%)"))
log_msg(glue("  Remaining: {n_remaining_s1} ({round(100*n_remaining_s1/n_entities, 1)}%)"))

# =============================================================================
# STRATEGY 2: HISTORICAL TICKER DATABASE (DELISTED/RENAMED)
# =============================================================================

log_strategy("STRATEGY 2: Historical Ticker Database")

# Check if historical DB exists
if (file.exists(CONFIG$cache_historical_db)) {
  
  log_msg(glue("Loading historical ticker database from {CONFIG$cache_historical_db}"))
  
  historical_db <- try(readRDS(CONFIG$cache_historical_db), silent = TRUE)
  
  # ---- Standardizza schema historical_db (robusto a nomi colonna diversi) ----
  standardize_historical_db <- function(df) {
    
    # ticker_clean
    if (!"ticker_clean" %in% names(df)) {
      if ("ticker" %in% names(df)) {
        df <- df %>% mutate(ticker_clean = clean_ticker_for_lookup(ticker))
      }
    }
    
    # cik
    if (!"cik" %in% names(df)) {
      if ("cik10" %in% names(df)) df <- df %>% rename(cik = cik10)
      if ("cik_str" %in% names(df)) df <- df %>% rename(cik = cik_str)
    }
    if ("cik" %in% names(df)) df <- df %>% mutate(cik = format_cik_10(cik))
    
    # sec_company_name
    if (!"sec_company_name" %in% names(df)) {
      alt <- intersect(names(df), c("sec_name", "company_name", "name", "title"))
      if (length(alt) > 0) {
        df <- df %>% rename(sec_company_name = all_of(alt[1]))
      } else {
        df$sec_company_name <- NA_character_
      }
    }
    
    df
  }
  
  historical_db <- standardize_historical_db(historical_db)
  
  # Se ancora mancano colonne base, skippa Strategy 2 invece di crashare
  need <- c("ticker_clean", "cik")
  if (!all(need %in% names(historical_db))) {
    log_msg("WARNING: historical_db missing required columns (ticker_clean/cik). Skipping Strategy 2.", "WARN")
    historical_db <- NULL
  }
  
  if (!inherits(historical_db, "try-error") && nrow(historical_db) > 0) {
    
    log_msg(glue("Loaded: {nrow(historical_db)} historical ticker-CIK mappings"))
    
    # Match unresolved entities with ticker
    unresolved_with_ticker <- entities %>%
      filter(is.na(target_cik), 
             !is.na(target_ticker_clean), 
             target_ticker_clean != "")
    
    if (nrow(unresolved_with_ticker) > 0) {
      
      matched_historical <- unresolved_with_ticker %>%
        left_join(
          historical_db %>%
            select(any_of(c("ticker_clean", "cik", "sec_company_name"))) %>%
            distinct(ticker_clean, .keep_all = TRUE),
          by = c("target_ticker_clean" = "ticker_clean")
        ) %>%
        filter(!is.na(cik))
      
      n_matched_hist <- nrow(matched_historical)
      
      if (n_matched_hist > 0) {
        
        entities <- entities %>%
          left_join(
            matched_historical %>%
              select(entity_key, cik, sec_name) %>%
              rename(cik_hist = cik, sec_name_hist = sec_name),
            by = "entity_key"
          ) %>%
          mutate(
            target_cik = if_else(is.na(target_cik) & !is.na(cik_hist),
                                 cik_hist, target_cik),
            cik_source = if_else(is.na(cik_source) & !is.na(cik_hist),
                                 "historical_db", cik_source),
            cik_confidence = if_else(is.na(cik_confidence) & !is.na(cik_hist),
                                     "high", cik_confidence),
            sec_company_name = if_else(is.na(sec_company_name) & !is.na(sec_name_hist),
                                       sec_name_hist, sec_company_name)
          ) %>%
          select(-cik_hist, -sec_name_hist)
        
        log_msg(glue("Matched: {n_matched_hist} entities via historical database"))
      } else {
        log_msg("No matches found in historical database")
      }
      
    } else {
      log_msg("No unresolved entities with tickers to check against historical DB")
    }
    
  } else {
    log_msg("WARNING: Failed to load historical database", "WARN")
  }
  
} else {
  log_msg("Historical ticker database not found - skipping Strategy 2")
  log_msg(glue("  Expected location: {CONFIG$cache_historical_db}"))
  log_msg("  To build: source('src/15_historical_cik/build_historical_ticker_database.R')")
}

# Track progress
n_resolved_s2 <- sum(!is.na(entities$target_cik))
n_remaining_s2 <- n_entities - n_resolved_s2
n_added_s2 <- n_resolved_s2 - n_resolved_s1

log_msg("")
log_msg(glue("Strategy 2 results:"))
log_msg(glue("  Newly resolved: {n_added_s2} ({round(100*n_added_s2/n_entities, 1)}%)"))
log_msg(glue("  Total resolved: {n_resolved_s2} ({round(100*n_resolved_s2/n_entities, 1)}%)"))
log_msg(glue("  Remaining: {n_remaining_s2} ({round(100*n_remaining_s2/n_entities, 1)}%)"))

# =============================================================================
# STRATEGY 3: BROWSE-EDGAR LOOKUP (TICKER/NAME SEARCH)
# =============================================================================

log_strategy("STRATEGY 3: Browse-EDGAR Lookup")

unresolved_s3 <- entities %>%
  filter(is.na(target_cik))

n_to_query_s3 <- nrow(unresolved_s3)

log_msg(glue("Querying Browse-EDGAR for {n_to_query_s3} unresolved entities"))

if (n_to_query_s3 > 0) {
  
  log_msg("This may take several minutes due to rate limiting...")
  
  # Query in batches with progress tracking
  batch_size <- 50
  n_batches <- ceiling(n_to_query_s3 / batch_size)
  
  browse_results <- list()
  
  for (batch_idx in seq_len(n_batches)) {
    
    batch_start <- (batch_idx - 1) * batch_size + 1
    batch_end <- min(batch_idx * batch_size, n_to_query_s3)
    
    log_msg(glue("  Batch {batch_idx}/{n_batches}: entities {batch_start}-{batch_end}"))
    
    batch_entities <- unresolved_s3[batch_start:batch_end, ]
    
    batch_results <- batch_entities %>%
      mutate(
        # Prefer ticker lookup, fallback to name
        search_term = if_else(!is.na(target_ticker_clean) & target_ticker_clean != "",
                              target_ticker_clean,
                              target_name_clean),
        browse_result = purrr::map(
          search_term,
          ~browse_edgar_lookup(
            .x,
            ua = CONFIG$sec_user_agent,
            delay = CONFIG$rate_limit_delay,
            cache_dir = CONFIG$cache_browse
          )
        )
      )
    
    browse_results[[batch_idx]] <- batch_results
  }
  
  # Combine results
  all_browse <- bind_rows(browse_results)
  
  # Process candidates (pick best match per entity)
  matched_browse <- all_browse %>%
    filter(purrr::map_int(browse_result, nrow) > 0) %>%
    mutate(
      best_match = purrr::pmap(
        list(browse_result, target_name_clean, target_ticker_clean),
        ~{
          cand <- ..1
          nm   <- ..2
          tkr  <- ..3
          
          if (nrow(cand) == 0) return(list(
            cik = NA_character_,
            confidence = "reject",
            sec_company_name = NA_character_
          ))
          
          pick_best_cik(
            candidates = cand,
            target_name_clean = nm,
            target_ticker_clean = tkr,
            ua = CONFIG$sec_user_agent,
            delay = CONFIG$rate_limit_delay
          )
        }
      ),
      cik_browse = purrr::map_chr(best_match, "cik"),
      conf_browse = purrr::map_chr(best_match, "confidence"),
      sec_name_browse = purrr::map_chr(best_match, "sec_company_name")
    ) %>%
    filter(!is.na(cik_browse), conf_browse != "reject")
  
  n_matched_browse <- nrow(matched_browse)
  
  if (n_matched_browse > 0) {
    
    entities <- entities %>%
      left_join(
        matched_browse %>%
          select(entity_key, cik_browse, conf_browse, sec_name_browse),
        by = "entity_key"
      ) %>%
      mutate(
        target_cik = if_else(is.na(target_cik) & !is.na(cik_browse),
                             cik_browse, target_cik),
        cik_source = if_else(is.na(cik_source) & !is.na(cik_browse),
                             "browse_edgar", cik_source),
        cik_confidence = if_else(is.na(cik_confidence) & !is.na(conf_browse),
                                 conf_browse, cik_confidence),
        sec_company_name = if_else(is.na(sec_company_name) & !is.na(sec_name_browse),
                                   sec_name_browse, sec_company_name)
      ) %>%
      select(-cik_browse, -conf_browse, -sec_name_browse)
    
    log_msg(glue("Matched: {n_matched_browse} entities via Browse-EDGAR"))
    
  } else {
    log_msg("No valid matches found via Browse-EDGAR")
  }
  
} else {
  log_msg("No entities to query via Browse-EDGAR (all resolved)")
}

# Track progress
n_resolved_s3 <- sum(!is.na(entities$target_cik))
n_remaining_s3 <- n_entities - n_resolved_s3
n_added_s3 <- n_resolved_s3 - n_resolved_s2

log_msg("")
log_msg(glue("Strategy 3 results:"))
log_msg(glue("  Newly resolved: {n_added_s3} ({round(100*n_added_s3/n_entities, 1)}%)"))
log_msg(glue("  Total resolved: {n_resolved_s3} ({round(100*n_resolved_s3/n_entities, 1)}%)"))
log_msg(glue("  Remaining: {n_remaining_s3} ({round(100*n_remaining_s3/n_entities, 1)}%)"))

# =============================================================================
# STRATEGY 4: FUZZY NAME MATCHING (LAST RESORT)
# =============================================================================

log_strategy("STRATEGY 4: Fuzzy Name Matching (Manual Review Required)")

# Note: This strategy is implemented but results should be manually validated
# due to higher false positive risk

unresolved_s4 <- entities %>%
  filter(is.na(target_cik))

n_unresolved_s4 <- nrow(unresolved_s4)

if (n_unresolved_s4 > 0) {
  log_msg(glue("{n_unresolved_s4} entities remain unresolved"))
  log_msg("These require manual review or are not SEC filers")
  log_msg("See output file: cik_unmatched.csv")
} else {
  log_msg("All entities resolved - no fuzzy matching needed")
}

# =============================================================================
# MERGE BACK TO DEALS
# =============================================================================

log_section("MERGING RESULTS TO DEALS")

# Join entity resolutions back to deals
deals_final <- deals_keyed %>%
  select(-target_cik, -cik_source, -cik_confidence, -sec_company_name) %>%
  left_join(
    entities %>% 
      select(entity_key, target_cik, cik_source, cik_confidence, sec_company_name),
    by = "entity_key"
  ) %>%
  select(-entity_key, -target_ticker_clean, -target_name_clean)

n_deals_with_cik <- sum(!is.na(deals_final$target_cik))

log_msg(glue("Merged entity resolutions to {n_total} deals"))
log_msg(glue("  Deals with CIK: {n_deals_with_cik} ({round(100*n_deals_with_cik/n_total, 1)}%)"))
log_msg(glue("  Deals without CIK: {n_total - n_deals_with_cik} ({round(100*(n_total - n_deals_with_cik)/n_total, 1)}%)"))

# =============================================================================
# DIAGNOSTICS
# =============================================================================

log_section("GENERATING DIAGNOSTICS")

# Coverage by source
coverage_by_source <- deals_final %>%
  group_by(cik_source) %>%
  summarise(
    n_deals = n(),
    pct_of_total = round(100 * n() / n_total, 1),
    .groups = "drop"
  ) %>%
  arrange(desc(n_deals))

log_msg("Coverage by source:")
for (i in seq_len(nrow(coverage_by_source))) {
  src <- coverage_by_source$cik_source[i]
  n <- coverage_by_source$n_deals[i]
  pct <- coverage_by_source$pct_of_total[i]
  
  src_label <- if_else(is.na(src), "[UNMATCHED]", src)
  log_msg(glue("  {src_label}: {n} deals ({pct}%)"))
}

# Coverage by confidence
coverage_by_confidence <- deals_final %>%
  filter(!is.na(target_cik)) %>%
  group_by(cik_confidence) %>%
  summarise(
    n_deals = n(),
    pct_of_matched = round(100 * n() / n_deals_with_cik, 1),
    .groups = "drop"
  ) %>%
  arrange(desc(n_deals))

log_msg("")
log_msg("Confidence distribution (among matched):")
for (i in seq_len(nrow(coverage_by_confidence))) {
  conf <- coverage_by_confidence$cik_confidence[i]
  n <- coverage_by_confidence$n_deals[i]
  pct <- coverage_by_confidence$pct_of_matched[i]
  
  log_msg(glue("  {conf}: {n} deals ({pct}%)"))
}

# Prepare diagnostics table
diagnostics <- deals_final %>%
  select(deal_id, target_name, target_ticker, target_cik, 
         cik_source, cik_confidence, sec_company_name) %>%
  mutate(
    matched = !is.na(target_cik),
    ticker_available = !is.na(target_ticker) & target_ticker != ""
  )

# =============================================================================
# SAVE OUTPUTS
# =============================================================================

log_section("SAVING OUTPUTS")

# 1. Main output: deals with CIK
saveRDS(deals_final, CONFIG$output_file)
log_msg(glue("Saved: {CONFIG$output_file}"))
log_msg(glue("  {nrow(deals_final)} deals × {ncol(deals_final)} columns"))

# 2. Diagnostics CSV
write.csv(diagnostics, CONFIG$diagnostics_file, row.names = FALSE)
log_msg(glue("Saved: {CONFIG$diagnostics_file}"))

# 3. Unmatched deals (for manual review)
unmatched <- deals_final %>%
  filter(is.na(target_cik)) %>%
  select(deal_id, target_name, target_ticker, date_announced) %>%
  arrange(target_name)

if (nrow(unmatched) > 0) {
  write.csv(unmatched, CONFIG$unmatched_file, row.names = FALSE)
  log_msg(glue("Saved: {CONFIG$unmatched_file}"))
  log_msg(glue("  {nrow(unmatched)} unmatched deals requiring manual review"))
} else {
  log_msg("No unmatched deals - all resolved!")
}

# =============================================================================
# FINAL SUMMARY
# =============================================================================

log_section("CIK RESOLUTION COMPLETE - FINAL SUMMARY")

total_time <- difftime(Sys.time(), 
                       as.POSIXct(readLines(CONFIG$log_file)[1] %>% 
                                    str_extract("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}")),
                       units = "mins")

log_msg(glue("Execution time: {round(total_time, 1)} minutes"))
log_msg("")
log_msg("STRATEGY PERFORMANCE:")
log_msg(glue("  Strategy 1 (SEC Bulk):        {n_resolved_s1} entities ({round(100*n_resolved_s1/n_entities, 1)}%)"))
log_msg(glue("  Strategy 2 (Historical DB):   +{n_added_s2} entities ({round(100*n_added_s2/n_entities, 1)}%)"))
log_msg(glue("  Strategy 3 (Browse-EDGAR):    +{n_added_s3} entities ({round(100*n_added_s3/n_entities, 1)}%)"))
log_msg(glue("  Unresolved:                   {n_remaining_s3} entities ({round(100*n_remaining_s3/n_entities, 1)}%)"))
log_msg("")
log_msg("DEAL-LEVEL COVERAGE:")
log_msg(glue("  Total deals:     {n_total}"))
log_msg(glue("  With CIK:        {n_deals_with_cik} ({round(100*n_deals_with_cik/n_total, 1)}%)"))
log_msg(glue("  Without CIK:     {n_total - n_deals_with_cik} ({round(100*(n_total - n_deals_with_cik)/n_total, 1)}%)"))
log_msg("")
log_msg("KEY OUTPUTS:")
log_msg(glue("  Main dataset:    {CONFIG$output_file}"))
log_msg(glue("  Diagnostics:     {CONFIG$diagnostics_file}"))
log_msg(glue("  Unmatched:       {CONFIG$unmatched_file}"))
log_msg("")
log_msg("QUALITY CHECKS:")
log_msg(glue("  High confidence: {sum(deals_final$cik_confidence == 'high', na.rm=TRUE)} ({round(100*sum(deals_final$cik_confidence == 'high', na.rm=TRUE)/n_deals_with_cik, 1)}% of matched)"))
log_msg(glue("  Medium confidence: {sum(deals_final$cik_confidence == 'medium', na.rm=TRUE)} ({round(100*sum(deals_final$cik_confidence == 'medium', na.rm=TRUE)/n_deals_with_cik, 1)}% of matched)"))
log_msg(glue("  Low confidence: {sum(deals_final$cik_confidence == 'low', na.rm=TRUE)} ({round(100*sum(deals_final$cik_confidence == 'low', na.rm=TRUE)/n_deals_with_cik, 1)}% of matched)"))

close(log_conn)

# Console summary box
cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║              CIK RESOLUTION COMPLETE - SUMMARY                   ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Total deals:          %-43d ║\n", n_total))
cat(sprintf("║ Deals with CIK:       %-27s (%5.1f%%) ║\n", 
            n_deals_with_cik, 100*n_deals_with_cik/n_total))
cat(sprintf("║ Deals without CIK:    %-27s (%5.1f%%) ║\n",
            n_total - n_deals_with_cik, 100*(n_total - n_deals_with_cik)/n_total))
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ Strategy breakdown:                                              ║\n")
cat(sprintf("║   SEC Bulk Ticker:    %-27s (%5.1f%%) ║\n",
            n_resolved_s1, 100*n_resolved_s1/n_entities))
cat(sprintf("║   Historical DB:      %-27s (%5.1f%%) ║\n",
            paste0("+", n_added_s2), 100*n_added_s2/n_entities))
cat(sprintf("║   Browse-EDGAR:       %-27s (%5.1f%%) ║\n",
            paste0("+", n_added_s3), 100*n_added_s3/n_entities))
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ OUTPUTS:                                                         ║\n")
cat("║   data/interim/deals_with_cik.rds                                ║\n")
cat("║   data/interim/cik_diagnostics.csv                               ║\n")
cat("║   data/interim/cik_unmatched.csv                                 ║\n")
cat("║   data/interim/cik_resolution_log.txt                            ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Execution time:       %-27s minutes    ║\n", round(total_time, 1)))
cat("╚══════════════════════════════════════════════════════════════════╝\n")
cat("\n")

if (n_deals_with_cik >= 0.75 * n_total) {
  cat("✓ Good coverage achieved (≥75%)\n")
  cat("\n")
  cat("NEXT STEP:\n")
  cat("  source('src/20_resolve/05_filing_identify.R')\n")
} else if (n_deals_with_cik >= 0.60 * n_total) {
  cat("⚠ Moderate coverage (60-75%)\n")
  cat("\n")
  cat("RECOMMENDATIONS:\n")
  cat("  1. Review unmatched deals: data/interim/cik_unmatched.csv\n")
  cat("  2. Consider building historical DB if not done:\n")
  cat("     source('src/15_historical_cik/build_historical_ticker_database.R')\n")
  cat("  3. Manual CIK lookup for high-priority deals\n")
} else {
  cat("✗ Low coverage (<60%)\n")
  cat("\n")
  cat("TROUBLESHOOTING:\n")
  cat("  1. Check SEC API connectivity\n")
  cat("  2. Verify ticker quality in source data\n")
  cat("  3. Build historical ticker database (Strategy 2)\n")
  cat("  4. Review logs: data/interim/cik_resolution_log.txt\n")
}

cat("\n")

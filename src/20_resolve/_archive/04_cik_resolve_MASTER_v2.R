# =============================================================================
# src/20_resolve/04_cik_resolve_MASTER.R
# =============================================================================
# MASTER CIK RESOLUTION PIPELINE
#
# STRATEGY CASCADE (in order):
#   1) SEC Bulk Ticker File (company_tickers.json)          [high]
#   2) Historical Ticker DB (historical_ticker_cik_database)[high]
#   3) Browse-EDGAR + Submissions validation                [high/med/low + review]
#
# INPUT:  data/processed/deals_restricted.rds
# OUTPUT: data/interim/deals_with_cik.rds
#         data/interim/cik_resolution_log.txt
#         data/interim/cik_diagnostics.csv
#         data/interim/cik_unmatched.csv
#         data/interim/cik_suspect_matches.csv
#
# DEPENDENCIES:
#   - src/99_utils/sec_http.R
#   - src/99_utils/sec_cik_lookup.R
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(glue)
  library(jsonlite)
  library(httr)
  library(tibble)
  library(purrr)
})

source("src/99_utils/sec_http.R")
source("src/99_utils/sec_cik_lookup.R")

CONFIG <- list(
  input_file = "data/processed/deals_restricted.rds",
  output_file = "data/interim/deals_with_cik.rds",
  diagnostics_file = "data/interim/cik_diagnostics.csv",
  unmatched_file = "data/interim/cik_unmatched.csv",
  suspect_file = "data/interim/cik_suspect_matches.csv",
  log_file = "data/interim/cik_resolution_log.txt",
  
  sec_user_agent = sec_user_agent(),
  sec_bulk_url = "https://www.sec.gov/files/company_tickers.json",
  rate_limit_delay = 0.15,
  
  cache_bulk = "data/interim/sec_cache/bulk_ticker.rds",
  cache_historical_db = "data/interim/historical_ticker_cik_database.rds",
  cache_submissions = "data/interim/sec_cache/submissions",
  cache_browse = "data/interim/sec_cache/browse_edgar",
  
  bulk_max_age_days = 7,
  browse_batch_size = 50,
  
  # Submissions-based acceptance is handled in pick_best_cik(); this threshold
  # is only used inside sec_cik_lookup.R as a fallback name-sim gate.
  name_similarity_min = 0.75,
  
  verbose = TRUE
)

dir.create(dirname(CONFIG$output_file), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(CONFIG$log_file), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(CONFIG$cache_bulk), recursive = TRUE, showWarnings = FALSE)
dir.create(CONFIG$cache_submissions, recursive = TRUE, showWarnings = FALSE)
dir.create(CONFIG$cache_browse, recursive = TRUE, showWarnings = FALSE)

# =============================================================================
# LOGGING (robust close on error)
# =============================================================================

# =============================================================================
# LOGGING (connectionless, robust)
# =============================================================================

start_time <- Sys.time()

dir.create(dirname(CONFIG$log_file), recursive = TRUE, showWarnings = FALSE)
# reset log file
cat("", file = CONFIG$log_file)

log_msg <- function(msg, level = "INFO") {
  ts <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  line <- glue("[{ts}] [{level}] {msg}")
  cat(line, "\n", file = CONFIG$log_file, append = TRUE)
  if (isTRUE(CONFIG$verbose)) cat(line, "\n")
}

log_section <- function(title) {
  line <- paste(rep("=", 70), collapse = "")
  log_msg("")
  log_msg(line)
  log_msg(title)
  log_msg(line)
}

log_strategy <- function(name) {
  line <- paste(rep("-", 70), collapse = "")
  log_msg("")
  log_msg(line)
  log_msg(glue("STRATEGY: {name}"))
  log_msg(line)
}
# =============================================================================
# INIT
# =============================================================================

log_section("CIK RESOLUTION - INITIALIZATION")

if (!file.exists(CONFIG$input_file)) {
  log_msg(glue("ERROR: Input file not found: {CONFIG$input_file}"), "ERROR")
  stop("Missing input file: ", CONFIG$input_file)
}

deals <- readRDS(CONFIG$input_file)
n_total <- nrow(deals)
log_msg(glue("Loaded {n_total} deals from {CONFIG$input_file}"))

required_cols <- c("deal_id", "target_name", "target_ticker", "date_announced")
missing_cols <- setdiff(required_cols, names(deals))
if (length(missing_cols) > 0) {
  log_msg(glue("ERROR: Missing required columns: {paste(missing_cols, collapse=', ')}"), "ERROR")
  stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
}

# Ensure output columns exist in deals (preserve if already present)
if (!("target_cik" %in% names(deals))) deals$target_cik <- NA_character_
if (!("cik_source" %in% names(deals))) deals$cik_source <- NA_character_
if (!("cik_confidence" %in% names(deals))) deals$cik_confidence <- NA_character_
if (!("sec_company_name" %in% names(deals))) deals$sec_company_name <- NA_character_

# =============================================================================
# ENTITY DEDUPLICATION (resolve once per entity)
# =============================================================================

log_section("ENTITY DEDUPLICATION")

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

entities <- deals_keyed %>%
  select(entity_key, target_name, target_name_clean, target_ticker, target_ticker_clean) %>%
  distinct(entity_key, .keep_all = TRUE) %>%
  mutate(
    target_cik = NA_character_,
    cik_source = NA_character_,
    cik_confidence = NA_character_,
    sec_company_name = NA_character_
  )

n_entities <- nrow(entities)
log_msg(glue("Unique entities: {n_entities}"))

# =============================================================================
# STRATEGY 1: SEC BULK TICKER FILE
# =============================================================================

log_strategy("SEC Bulk Ticker File")

use_cache <- FALSE
if (file.exists(CONFIG$cache_bulk)) {
  age_days <- as.numeric(difftime(Sys.time(), file.info(CONFIG$cache_bulk)$mtime, units = "days"))
  if (!is.na(age_days) && age_days <= CONFIG$bulk_max_age_days) use_cache <- TRUE
}

if (!use_cache) {
  log_msg("Fetching SEC bulk ticker file...")
  bulk_resp <- try(sec_get(CONFIG$sec_bulk_url, ua = CONFIG$sec_user_agent, delay = CONFIG$rate_limit_delay), silent = TRUE)
  
  if (inherits(bulk_resp, "try-error")) {
    log_msg("WARNING: failed to fetch bulk ticker file; skipping Strategy 1", "WARN")
    bulk_tickers <- NULL
  } else {
    bulk_txt <- httr::content(bulk_resp, as = "text", encoding = "UTF-8")
    bulk_json <- jsonlite::fromJSON(bulk_txt, simplifyVector = TRUE)
    
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
      transmute(ticker_clean, cik, sec_name = title) %>%
      distinct(ticker_clean, .keep_all = TRUE)
    
    saveRDS(bulk_tickers, CONFIG$cache_bulk)
    log_msg(glue("Cached bulk tickers: {nrow(bulk_tickers)}"))
  }
} else {
  bulk_tickers <- readRDS(CONFIG$cache_bulk)
  log_msg(glue("Loaded cached bulk tickers: {nrow(bulk_tickers)}"))
}

if (!is.null(bulk_tickers) && nrow(bulk_tickers) > 0) {
  matched_bulk <- deals_keyed %>%
    filter(!is.na(target_ticker_clean), target_ticker_clean != "") %>%
    left_join(bulk_tickers, by = c("target_ticker_clean" = "ticker_clean")) %>%
    filter(!is.na(cik)) %>%
    select(entity_key, cik, sec_name) %>%
    distinct(entity_key, .keep_all = TRUE)
  
  if (nrow(matched_bulk) > 0) {
    entities <- entities %>%
      left_join(
        matched_bulk %>% rename(cik_bulk = cik, sec_name_bulk = sec_name),
        by = "entity_key"
      ) %>%
      mutate(
        target_cik = if_else(is.na(target_cik) & !is.na(cik_bulk), cik_bulk, target_cik),
        cik_source = if_else(is.na(cik_source) & !is.na(cik_bulk), "sec_bulk", cik_source),
        cik_confidence = if_else(is.na(cik_confidence) & !is.na(cik_bulk), "high", cik_confidence),
        sec_company_name = if_else(is.na(sec_company_name) & !is.na(sec_name_bulk), sec_name_bulk, sec_company_name)
      ) %>%
      select(-any_of(c("cik_bulk", "sec_name_bulk")))
    
    log_msg(glue("Matched via bulk: {nrow(matched_bulk)} entities"))
  } else {
    log_msg("No matches via bulk")
  }
} else {
  log_msg("No bulk ticker data available", "WARN")
}

n_resolved_s1 <- sum(!is.na(entities$target_cik))
log_msg(glue("After Strategy 1 resolved: {n_resolved_s1}/{n_entities} ({round(100*n_resolved_s1/n_entities, 1)}%)"))

# =============================================================================
# STRATEGY 2: HISTORICAL TICKER DB
# =============================================================================

log_strategy("Historical Ticker DB")

standardize_historical_db <- function(df) {
  df <- as_tibble(df)
  
  if (!"ticker_clean" %in% names(df)) {
    if ("ticker" %in% names(df)) df <- df %>% mutate(ticker_clean = clean_ticker_for_lookup(ticker))
  } else {
    df <- df %>% mutate(ticker_clean = clean_ticker_for_lookup(ticker_clean))
  }
  
  if (!"cik" %in% names(df)) {
    if ("cik10" %in% names(df)) df <- df %>% rename(cik = cik10)
    if ("cik_str" %in% names(df)) df <- df %>% rename(cik = cik_str)
  }
  if ("cik" %in% names(df)) df <- df %>% mutate(cik = format_cik_10(cik))
  
  # normalize company name column to sec_company_name
  if (!"sec_company_name" %in% names(df)) {
    if ("sec_name" %in% names(df)) {
      df <- df %>% rename(sec_company_name = sec_name)
    } else {
      alt <- intersect(names(df), c("company_name", "name", "title"))
      if (length(alt) > 0) df <- df %>% rename(sec_company_name = all_of(alt[1])) else df$sec_company_name <- NA_character_
    }
  }
  
  need <- c("ticker_clean", "cik")
  if (!all(need %in% names(df))) return(NULL)
  
  df %>%
    filter(!is.na(ticker_clean), ticker_clean != "", !is.na(cik)) %>%
    select(any_of(c("ticker_clean", "cik", "sec_company_name"))) %>%
    distinct(ticker_clean, .keep_all = TRUE)
}

if (file.exists(CONFIG$cache_historical_db)) {
  historical_db <- try(readRDS(CONFIG$cache_historical_db), silent = TRUE)
  if (inherits(historical_db, "try-error")) {
    log_msg("WARNING: failed to read historical DB; skipping Strategy 2", "WARN")
    historical_db <- NULL
  } else {
    historical_db <- standardize_historical_db(historical_db)
  }
  
  if (!is.null(historical_db) && nrow(historical_db) > 0) {
    unresolved_with_ticker <- entities %>%
      filter(is.na(target_cik), !is.na(target_ticker_clean), target_ticker_clean != "")
    
    if (nrow(unresolved_with_ticker) > 0) {
      
      matched_hist <- unresolved_with_ticker %>%
        mutate(ticker_candidates = map(target_ticker_clean, ticker_variants)) %>%
        mutate(
          hist_hit = map(ticker_candidates, ~{
            tc <- .x
            if (length(tc) == 0) return(tibble(cik = NA_character_, sec_company_name = NA_character_))
            hit <- historical_db %>% filter(ticker_clean %in% tc)
            if (nrow(hit) == 0) return(tibble(cik = NA_character_, sec_company_name = NA_character_))
            hit[1, c("cik", "sec_company_name")]
          })
        ) %>%
        mutate(
          cik_hist = map_chr(hist_hit, ~.x$cik[1]),
          sec_company_name_hist = map_chr(hist_hit, ~.x$sec_company_name[1])
        ) %>%
        filter(!is.na(cik_hist)) %>%
        select(entity_key, cik_hist, sec_company_name_hist) %>%
        distinct(entity_key, .keep_all = TRUE)
      
      if (nrow(matched_hist) > 0) {
        entities <- entities %>%
          left_join(matched_hist, by = "entity_key") %>%
          mutate(
            target_cik = if_else(is.na(target_cik) & !is.na(cik_hist), cik_hist, target_cik),
            cik_source = if_else(is.na(cik_source) & !is.na(cik_hist), "historical_db", cik_source),
            cik_confidence = if_else(is.na(cik_confidence) & !is.na(cik_hist), "high", cik_confidence),
            sec_company_name = if_else(is.na(sec_company_name) & !is.na(sec_company_name_hist),
                                       sec_company_name_hist, sec_company_name)
          ) %>%
          select(-any_of(c("cik_hist", "sec_company_name_hist")))
        
        log_msg(glue("Matched via historical DB: {nrow(matched_hist)} entities"))
      } else {
        log_msg("No matches via historical DB")
      }
    } else {
      log_msg("No unresolved entities with tickers for Strategy 2")
    }
  } else {
    log_msg("Historical DB empty/invalid; skipping Strategy 2", "WARN")
  }
} else {
  log_msg(glue("Historical DB not found: {CONFIG$cache_historical_db}"), "WARN")
}

n_resolved_s2 <- sum(!is.na(entities$target_cik))
n_added_s2 <- n_resolved_s2 - n_resolved_s1
log_msg(glue("After Strategy 2 newly resolved: {n_added_s2} | total resolved: {n_resolved_s2}/{n_entities} ({round(100*n_resolved_s2/n_entities, 1)}%)"))

# =============================================================================
# STRATEGY 3: BROWSE-EDGAR + SUBMISSIONS VALIDATION
# =============================================================================

log_strategy("Browse-EDGAR + Submissions validation")

unresolved_s3 <- entities %>% filter(is.na(target_cik))
n_to_query <- nrow(unresolved_s3)
log_msg(glue("Entities to query: {n_to_query}"))

if (n_to_query > 0) {
  
  batch_size <- CONFIG$browse_batch_size
  n_batches <- ceiling(n_to_query / batch_size)
  browse_results <- vector("list", n_batches)
  
  for (b in seq_len(n_batches)) {
    i0 <- (b - 1) * batch_size + 1
    i1 <- min(b * batch_size, n_to_query)
    log_msg(glue("Batch {b}/{n_batches}: entities {i0}-{i1}"))
    
    batch_entities <- unresolved_s3[i0:i1, ]
    
    batch_out <- batch_entities %>%
      mutate(
        browse_result = pmap(
          list(target_ticker_clean, target_name_clean),
          ~browse_candidates_ticker_then_name(
            ticker_clean = ..1,
            name_clean = ..2,
            ua = CONFIG$sec_user_agent,
            delay = CONFIG$rate_limit_delay,
            cache_dir = CONFIG$cache_browse
          )
        ),
        best_match = pmap(
          list(browse_result, target_name_clean, target_ticker_clean),
          ~pick_best_cik(
            candidates = std_candidate_schema(..1),
            target_name_clean = ..2,
            target_ticker_clean = ..3,
            ua = CONFIG$sec_user_agent,
            delay = CONFIG$rate_limit_delay,
            min_name_sim = CONFIG$name_similarity_min,
            incremental_historical_db_path = CONFIG$cache_historical_db
          )
        ),
        cik_browse = map_chr(best_match, "cik"),
        conf_browse = map_chr(best_match, "confidence"),
        sec_company_name_browse = map_chr(best_match, "sec_company_name")
      ) %>%
      filter(!is.na(cik_browse), conf_browse != "reject") %>%
      select(entity_key, cik_browse, conf_browse, sec_company_name_browse) %>%
      distinct(entity_key, .keep_all = TRUE)
    
    browse_results[[b]] <- batch_out
  }
  
  matched_browse <- bind_rows(browse_results)
  
  if (nrow(matched_browse) > 0) {
    entities <- entities %>%
      left_join(matched_browse, by = "entity_key") %>%
      mutate(
        target_cik = if_else(is.na(target_cik) & !is.na(cik_browse), cik_browse, target_cik),
        cik_source = if_else(is.na(cik_source) & !is.na(cik_browse), "browse_edgar", cik_source),
        cik_confidence = if_else(is.na(cik_confidence) & !is.na(conf_browse), conf_browse, cik_confidence),
        sec_company_name = if_else(is.na(sec_company_name) & !is.na(sec_company_name_browse),
                                   sec_company_name_browse, sec_company_name)
      ) %>%
      select(-any_of(c("cik_browse", "conf_browse", "sec_company_name_browse")))
    
    log_msg(glue("Matched via Browse-EDGAR: {nrow(matched_browse)} entities"))
  } else {
    log_msg("No valid matches via Browse-EDGAR")
  }
}

n_resolved_s3 <- sum(!is.na(entities$target_cik))
n_added_s3 <- n_resolved_s3 - n_resolved_s2
n_remaining <- n_entities - n_resolved_s3
log_msg(glue("After Strategy 3 newly resolved: {n_added_s3} | total resolved: {n_resolved_s3}/{n_entities} ({round(100*n_resolved_s3/n_entities, 1)}%)"))
log_msg(glue("Remaining unresolved entities: {n_remaining}"))

# =============================================================================
# MERGE BACK TO DEALS
# =============================================================================

log_section("MERGING RESULTS TO DEALS")

deals_final <- deals_keyed %>%
  select(-any_of(c("target_cik", "cik_source", "cik_confidence", "sec_company_name"))) %>%
  left_join(
    entities %>% select(entity_key, target_cik, cik_source, cik_confidence, sec_company_name),
    by = "entity_key"
  ) %>%
  select(-any_of(c("entity_key", "target_ticker_clean", "target_name_clean")))

n_deals_with_cik <- sum(!is.na(deals_final$target_cik))
log_msg(glue("Deals with CIK: {n_deals_with_cik}/{n_total} ({round(100*n_deals_with_cik/n_total, 1)}%)"))

# =============================================================================
# DIAGNOSTICS + EXPORTS
# =============================================================================

log_section("DIAGNOSTICS + EXPORTS")

diagnostics <- deals_final %>%
  transmute(
    deal_id,
    target_name,
    target_ticker,
    date_announced,
    target_cik,
    cik_source,
    cik_confidence,
    sec_company_name,
    matched = !is.na(target_cik),
    manual_review = case_when(
      is.na(target_cik) ~ TRUE,
      cik_confidence %in% c("low") ~ TRUE,
      TRUE ~ FALSE
    )
  )

unmatched <- diagnostics %>% filter(is.na(target_cik)) %>% arrange(target_name)
suspects <- diagnostics %>% filter(!is.na(target_cik), manual_review) %>% arrange(target_name)

saveRDS(deals_final, CONFIG$output_file)
write.csv(diagnostics, CONFIG$diagnostics_file, row.names = FALSE)
write.csv(unmatched, CONFIG$unmatched_file, row.names = FALSE)
write.csv(suspects, CONFIG$suspect_file, row.names = FALSE)

log_msg(glue("Saved: {CONFIG$output_file}"))
log_msg(glue("Saved: {CONFIG$diagnostics_file}"))
log_msg(glue("Saved: {CONFIG$unmatched_file} (n={nrow(unmatched)})"))
log_msg(glue("Saved: {CONFIG$suspect_file} (n={nrow(suspects)})"))

# =============================================================================
# FINAL SUMMARY
# =============================================================================

log_section("FINAL SUMMARY")

total_time <- difftime(Sys.time(), start_time, units = "mins")
log_msg(glue("Execution time: {round(total_time, 1)} minutes"))
log_msg(glue("Entity coverage: {n_resolved_s3}/{n_entities} ({round(100*n_resolved_s3/n_entities, 1)}%)"))
log_msg(glue("Deal coverage: {n_deals_with_cik}/{n_total} ({round(100*n_deals_with_cik/n_total, 1)}%)"))
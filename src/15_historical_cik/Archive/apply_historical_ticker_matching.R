# =============================================================================
# apply_historical_ticker_matching.R
# =============================================================================
# Apply historical ticker-CIK mappings to M&A deals
#
# This script takes the historical ticker database and intelligently matches
# each deal to the correct CIK based on ticker + company name validation.
#
# Key Innovation: Company name similarity scoring to validate ticker matches
# and distinguish between ticker recycling cases.
#
# Output: deals_with_cik_historical.rds (replaces bulk-only matching)
#
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(glue)
  library(stringdist)  # For fuzzy name matching
})

# =============================================================================
# CONFIGURATION
# =============================================================================

INPUT_DEALS <- "data/interim/deals_sample_restricted.rds"
HISTORICAL_DB <- "data/interim/historical_ticker_cik_database.rds"
BULK_MATCHES <- "data/interim/deals_with_cik_verified.rds"  # For comparison

OUTPUT_FILE <- "data/interim/deals_with_cik_historical.rds"
LOG_FILE <- "data/interim/historical_matching_log.txt"
REPORT_FILE <- "data/interim/historical_matching_report.txt"

# Fuzzy matching threshold (0-1, higher = stricter)
NAME_SIMILARITY_THRESHOLD <- 0.6

# =============================================================================
# SETUP
# =============================================================================

log_conn <- file(LOG_FILE, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  message(full_msg)
}

log_msg("=================================================================")
log_msg("HISTORICAL TICKER-CIK MATCHING")
log_msg("=================================================================")

# =============================================================================
# LOAD DATA
# =============================================================================

log_msg("Loading data...", "INFO")

deals <- readRDS(INPUT_DEALS)
historical_db <- readRDS(HISTORICAL_DB)
bulk_matches <- readRDS(BULK_MATCHES)

log_msg(glue("Deals: {nrow(deals)}"), "INFO")
log_msg(glue("Historical DB: {nrow(historical_db)} mappings"), "INFO")
log_msg(glue("Bulk matches (for comparison): {sum(!is.na(bulk_matches$target_cik))}"), "INFO")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

#' Calculate company name similarity score
#' 
#' Uses Jaro-Winkler distance to compute similarity between deal company name
#' and SEC company name. Returns score 0-1 (1 = perfect match).
#'
calculate_name_similarity <- function(name1, name2) {
  if (is.na(name1) || is.na(name2) || name1 == "" || name2 == "") {
    return(0)
  }
  
  # Normalize names
  norm1 <- toupper(trimws(name1))
  norm2 <- toupper(trimws(name2))
  
  # Remove common suffixes/prefixes that don't affect identity
  norm1 <- str_remove_all(norm1, "\\s+(INC\\.?|CORP\\.?|CORPORATION|LTD\\.?|LLC|LP|THE)$")
  norm2 <- str_remove_all(norm2, "\\s+(INC\\.?|CORP\\.?|CORPORATION|LTD\\.?|LLC|LP|THE)$")
  
  # Compute Jaro-Winkler similarity
  similarity <- 1 - stringdist(norm1, norm2, method = "jw")
  
  return(similarity)
}

#' Match a single deal to CIK using historical database + name validation
#'
match_deal_to_cik <- function(ticker, company_name, historical_db, threshold = 0.6) {
  
  if (is.na(ticker) || ticker == "") {
    return(list(
      cik = NA_character_,
      sec_company_name = NA_character_,
      match_method = "no_ticker",
      name_similarity = NA_real_
    ))
  }
  
  # Standardize ticker
  ticker_clean <- toupper(trimws(ticker))
  
  # Find all CIKs that have used this ticker
  candidates <- historical_db %>%
    filter(ticker == ticker_clean)
  
  if (nrow(candidates) == 0) {
    return(list(
      cik = NA_character_,
      sec_company_name = NA_character_,
      match_method = "ticker_not_found",
      name_similarity = NA_real_
    ))
  }
  
  # Single candidate - use it if name similarity acceptable
  if (nrow(candidates) == 1) {
    candidate <- candidates[1, ]
    similarity <- calculate_name_similarity(company_name, candidate$company_name)
    
    if (similarity >= threshold || is.na(company_name)) {
      return(list(
        cik = candidate$cik,
        sec_company_name = candidate$company_name,
        match_method = "unique_ticker",
        name_similarity = similarity
      ))
    } else {
      return(list(
        cik = NA_character_,
        sec_company_name = NA_character_,
        match_method = "name_mismatch",
        name_similarity = similarity
      ))
    }
  }
  
  # Multiple candidates - ticker recycling case
  # Use company name to disambiguate
  candidates <- candidates %>%
    mutate(
      similarity = sapply(company_name, function(sec_name) {
        calculate_name_similarity(company_name, sec_name)
      })
    ) %>%
    arrange(desc(similarity))
  
  best_match <- candidates[1, ]
  
  if (best_match$similarity >= threshold) {
    return(list(
      cik = best_match$cik,
      sec_company_name = best_match$company_name,
      match_method = "name_disambiguated",
      name_similarity = best_match$similarity
    ))
  }
  
  # No good match found
  return(list(
    cik = NA_character_,
    sec_company_name = NA_character_,
    match_method = "ambiguous_ticker",
    name_similarity = best_match$similarity
  ))
}

# =============================================================================
# APPLY MATCHING
# =============================================================================

log_msg("", "INFO")
log_msg("Applying historical ticker matching...", "INFO")

pb <- txtProgressBar(min = 0, max = nrow(deals), style = 3)

results <- list()

for (i in seq_len(nrow(deals))) {
  deal <- deals[i, ]
  
  match_result <- match_deal_to_cik(
    ticker = deal$target_ticker,
    company_name = deal$target_name,
    historical_db = historical_db,
    threshold = NAME_SIMILARITY_THRESHOLD
  )
  
  result_row <- deal
  result_row$target_cik <- match_result$cik
  result_row$sec_company_name <- match_result$sec_company_name
  result_row$cik_match_method <- match_result$match_method
  result_row$name_similarity_score <- match_result$name_similarity
  
  results[[i]] <- result_row
  
  setTxtProgressBar(pb, i)
}

close(pb)

deals_matched <- bind_rows(results)

# =============================================================================
# STATISTICS
# =============================================================================

log_msg("", "INFO")
log_msg("Computing match statistics...", "INFO")

n_total <- nrow(deals_matched)
n_matched <- sum(!is.na(deals_matched$target_cik))
pct_matched <- round(100 * n_matched / n_total, 1)

# Compare to bulk matching
n_bulk_matched <- sum(!is.na(bulk_matches$target_cik))
n_new_matches <- n_matched - n_bulk_matched
pct_improvement <- round(100 * n_new_matches / n_bulk_matched, 1)

log_msg("", "INFO")
log_msg("MATCH RESULTS:", "INFO")
log_msg(glue("  Total deals: {n_total}"), "INFO")
log_msg(glue("  Matched (historical): {n_matched} ({pct_matched}%)"), "INFO")
log_msg(glue("  Bulk matching: {n_bulk_matched} (13.8%)"), "INFO")
log_msg(glue("  New matches: {n_new_matches} (+{pct_improvement}% improvement)"), "INFO")

# Breakdown by method
method_summary <- deals_matched %>%
  count(cik_match_method) %>%
  mutate(pct = round(100 * n / n_total, 1)) %>%
  arrange(desc(n))

log_msg("", "INFO")
log_msg("Match method breakdown:", "INFO")
for (i in seq_len(nrow(method_summary))) {
  row <- method_summary[i, ]
  log_msg(glue("  {row$cik_match_method}: {row$n} ({row$pct}%)"), "INFO")
}

# Name similarity distribution (for matched deals)
matched_deals <- deals_matched %>%
  filter(!is.na(target_cik), !is.na(name_similarity_score))

if (nrow(matched_deals) > 0) {
  sim_stats <- summary(matched_deals$name_similarity_score)
  log_msg("", "INFO")
  log_msg("Name similarity scores (matched deals):", "INFO")
  log_msg(glue("  Min: {round(sim_stats[1], 2)}"), "INFO")
  log_msg(glue("  Median: {round(sim_stats[3], 2)}"), "INFO")
  log_msg(glue("  Mean: {round(sim_stats[4], 2)}"), "INFO")
  log_msg(glue("  Max: {round(sim_stats[6], 2)}"), "INFO")
}

# =============================================================================
# VALIDATION: Compare with Bulk Matches
# =============================================================================

log_msg("", "INFO")
log_msg("Validating against bulk matches...", "INFO")

# Join to compare CIK assignments
comparison <- deals_matched %>%
  select(target_ticker, target_name, target_cik_historical = target_cik, cik_match_method) %>%
  left_join(
    bulk_matches %>% select(target_ticker, target_name, target_cik_bulk = target_cik),
    by = c("target_ticker", "target_name")
  )

# Check agreement
agreement <- comparison %>%
  filter(!is.na(target_cik_historical), !is.na(target_cik_bulk)) %>%
  mutate(agrees = target_cik_historical == target_cik_bulk)

n_both_matched <- nrow(agreement)
n_agrees <- sum(agreement$agrees, na.rm = TRUE)
n_disagrees <- n_both_matched - n_agrees

log_msg(glue("Deals matched by both methods: {n_both_matched}"), "INFO")
log_msg(glue("  Agreements: {n_agrees}"), "INFO")
log_msg(glue("  Disagreements: {n_disagrees}"), "INFO")

if (n_disagrees > 0) {
  log_msg("", "WARN")
  log_msg("DISAGREEMENTS (first 10):", "WARN")
  disagreements <- agreement %>%
    filter(!agrees) %>%
    head(10)
  
  for (i in seq_len(nrow(disagreements))) {
    row <- disagreements[i, ]
    log_msg(glue("  {row$target_ticker}: Historical={row$target_cik_historical} vs Bulk={row$target_cik_bulk}"), "WARN")
  }
}

# =============================================================================
# SAVE OUTPUTS
# =============================================================================

log_msg("", "INFO")
log_msg("Saving outputs...", "INFO")

saveRDS(deals_matched, OUTPUT_FILE)
log_msg(glue("Saved: {OUTPUT_FILE}"), "INFO")

# Generate report
report_lines <- c(
  "=======================================================================",
  "HISTORICAL TICKER-CIK MATCHING REPORT",
  "=======================================================================",
  "",
  "SUMMARY:",
  glue("  Total deals: {n_total}"),
  glue("  Matched: {n_matched} ({pct_matched}%)"),
  glue("  Unmatched: {n_total - n_matched}"),
  "",
  "COMPARISON WITH BULK MATCHING:",
  glue("  Bulk matches: {n_bulk_matched} (13.8%)"),
  glue("  Historical matches: {n_matched} ({pct_matched}%)"),
  glue("  New matches gained: {n_new_matches} (+{pct_improvement}%)"),
  "",
  "MATCH METHOD BREAKDOWN:",
  ""
)

for (i in seq_len(nrow(method_summary))) {
  row <- method_summary[i, ]
  report_lines <- c(report_lines, glue("  {row$cik_match_method}: {row$n} ({row$pct}%)"))
}

report_lines <- c(
  report_lines,
  "",
  "VALIDATION:",
  glue("  Both methods matched: {n_both_matched} deals"),
  glue("  Agreements: {n_agrees}"),
  glue("  Disagreements: {n_disagrees}"),
  "",
  "======================================================================="
)

writeLines(report_lines, REPORT_FILE)
log_msg(glue("Saved: {REPORT_FILE}"), "INFO")

# =============================================================================
# COMPLETION
# =============================================================================

log_msg("", "INFO")
log_msg("=================================================================")
log_msg("HISTORICAL TICKER MATCHING COMPLETE")
log_msg("=================================================================")
log_msg(glue("Match rate: {n_bulk_matched} → {n_matched} (+{pct_improvement}%)"))
log_msg(glue("Output: {OUTPUT_FILE}"))

close(log_conn)

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("HISTORICAL TICKER MATCHING COMPLETE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat(glue("Bulk matching: {n_bulk_matched} deals (13.8%)"), "\n")
cat(glue("Historical matching: {n_matched} deals ({pct_matched}%)"), "\n")
cat(glue("Improvement: +{n_new_matches} deals (+{pct_improvement}%)"), "\n")
cat("\n")
cat(glue("See {REPORT_FILE} for detailed breakdown"), "\n")
cat("\n")
cat("Next: Rerun filing identification with improved CIK matches\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

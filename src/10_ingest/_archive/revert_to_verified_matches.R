# =============================================================================
# EMERGENCY FIX: Revert to Verified Bulk Matches Only
# =============================================================================
# The EDGAR search approach produced incorrect matches.
# This script returns to using ONLY the verified current SEC ticker matches.
#
# Usage: source("src/10_ingest/revert_to_verified_matches.R")
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(jsonlite)
  library(httr)
  library(stringr)
  library(glue)
})

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("REVERTING TO VERIFIED MATCHES ONLY\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")
cat("The EDGAR search produced incorrect matches.\n")
cat("Returning to bulk SEC matches only (verified correct).\n")
cat("\n")

# Load restricted deals
deals <- readRDS("data/interim/deals_sample_restricted.rds")
cat(glue("Loaded {nrow(deals)} restricted deals"), "\n\n")

# Download current SEC tickers (bulk method - verified correct)
cat("Downloading current SEC company tickers...\n")

sec_response <- httr::GET(
  "https://www.sec.gov/files/company_tickers.json",
  httr::add_headers(`User-Agent` = "mna-disclosure-research giuseppe.lando@studbocconi.it"),
  httr::timeout(30)
)

if (httr::http_error(sec_response)) {
  stop("Failed to download SEC ticker file")
}

sec_data <- httr::content(sec_response, as = "text", encoding = "UTF-8") %>%
  jsonlite::fromJSON(simplifyDataFrame = TRUE)

if (is.list(sec_data) && !is.data.frame(sec_data)) {
  sec_data <- dplyr::bind_rows(sec_data)
}

# Create verified mapping (current tickers only)
ticker_mapping <- sec_data %>%
  transmute(
    ticker = toupper(trimws(ticker)),
    target_cik = str_pad(as.character(cik_str), width = 10, side = "left", pad = "0"),
    sec_company_name = title
  ) %>%
  distinct(ticker, .keep_all = TRUE)

cat(glue("SEC mapping contains {nrow(ticker_mapping)} current tickers"), "\n\n")

# Match deals (bulk only - no EDGAR search)
deals_verified <- deals %>%
  mutate(ticker_clean = toupper(trimws(target_ticker))) %>%
  left_join(
    ticker_mapping,
    by = c("ticker_clean" = "ticker")
  ) %>%
  mutate(
    cik_match_status = case_when(
      !is.na(target_cik) ~ "matched",
      is.na(target_ticker) | target_ticker == "" ~ "no_ticker",
      TRUE ~ "ticker_not_in_sec"
    )
  ) %>%
  select(-ticker_clean)

# Statistics
n_matched <- sum(!is.na(deals_verified$target_cik))
n_unmatched <- sum(is.na(deals_verified$target_cik))
pct_matched <- round(100 * n_matched / nrow(deals), 1)

cat("VERIFIED MATCH STATISTICS:\n")
cat(glue("  Total deals: {nrow(deals)}"), "\n")
cat(glue("  Matched (verified): {n_matched} ({pct_matched}%)"), "\n")
cat(glue("  Unmatched: {n_unmatched}"), "\n\n")

# Save verified dataset
saveRDS(deals_verified, "data/interim/deals_with_cik_verified.rds")
cat(glue("✓ Saved: data/interim/deals_with_cik_verified.rds"), "\n\n")

# Temporal distribution check
cat("TEMPORAL DISTRIBUTION OF MATCHED DEALS:\n")
temporal_dist <- deals_verified %>%
  filter(!is.na(target_cik)) %>%
  mutate(year = lubridate::year(announce_date)) %>%
  count(year) %>%
  arrange(year)

print(temporal_dist, n = Inf)

cat("\n")

# Industry distribution check
cat("\nINDUSTRY DISTRIBUTION OF MATCHED DEALS:\n")
industry_dist <- deals_verified %>%
  filter(!is.na(target_cik)) %>%
  mutate(sic_2digit = substr(target_sic, 1, 2)) %>%
  count(sic_2digit, sort = TRUE) %>%
  head(10)

print(industry_dist)

cat("\n")

# Outcome distribution
cat("\nOUTCOME DISTRIBUTION OF MATCHED DEALS:\n")
outcome_dist <- deals_verified %>%
  filter(!is.na(target_cik)) %>%
  count(deal_completed) %>%
  mutate(
    outcome = if_else(deal_completed == 1, "Completed", "Withdrawn"),
    pct = round(100 * n / sum(n), 1)
  ) %>%
  select(outcome, n, pct)

print(outcome_dist)

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("VERIFIED DATASET READY\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat(glue("Working sample: {n_matched} deals with verified CIK matches"), "\n")
cat("Ready to proceed with filing identification.\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

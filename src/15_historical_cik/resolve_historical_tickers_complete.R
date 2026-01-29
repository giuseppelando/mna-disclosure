# =============================================================================
# resolve_historical_tickers_complete.R
# =============================================================================
# Master script: Complete historical ticker resolution pipeline
#
# This script orchestrates the full historical ticker resolution process:
#   1. Build historical ticker-CIK database from SEC data
#   2. Apply intelligent matching with name validation
#   3. Generate comprehensive diagnostics
#
# Run this single script to replace bulk-only CIK matching with
# historical ticker resolution that dramatically improves match rates.
#
# Expected improvement: 13.8% → 60-80% match rate
#
# Usage:
#   source("src/15_historical_cik/resolve_historical_tickers_complete.R")
#
# =============================================================================

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("HISTORICAL TICKER RESOLUTION - COMPLETE PIPELINE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")
cat("This process will:\n")
cat("  1. Build historical ticker-CIK database from SEC submissions\n")
cat("  2. Apply intelligent matching with company name validation\n")
cat("  3. Generate comprehensive match statistics\n")
cat("\n")
cat("Expected outcome: ~60-80% CIK match rate (vs 13.8% bulk-only)\n")
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

# Check prerequisites
if (!file.exists("data/interim/deals_sample_restricted.rds")) {
  stop("Missing: data/interim/deals_sample_restricted.rds\nRun sample restriction first.")
}

if (!file.exists("data/interim/deals_with_cik_verified.rds")) {
  stop("Missing: data/interim/deals_with_cik_verified.rds\nRun bulk CIK matching first.")
}

# PHASE 1: Build Historical Database
cat("PHASE 1: Building historical ticker database...\n")
cat(paste(rep("-", 70), collapse = ""), "\n")

source("src/15_historical_cik/build_historical_ticker_database.R")

if (!file.exists("data/interim/historical_ticker_cik_database.rds")) {
  stop("Phase 1 failed: Historical database not created")
}

cat("\n✓ Phase 1 complete\n\n")

# PHASE 2: Apply Historical Matching
cat("PHASE 2: Applying historical ticker matching...\n")
cat(paste(rep("-", 70), collapse = ""), "\n")

source("src/15_historical_cik/apply_historical_ticker_matching.R")

if (!file.exists("data/interim/deals_with_cik_historical.rds")) {
  stop("Phase 2 failed: Historical matches not created")
}

cat("\n✓ Phase 2 complete\n\n")

# FINAL SUMMARY
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("HISTORICAL TICKER RESOLUTION COMPLETE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

# Load results for summary
historical_matches <- readRDS("data/interim/deals_with_cik_historical.rds")
bulk_matches <- readRDS("data/interim/deals_with_cik_verified.rds")

n_total <- nrow(historical_matches)
n_historical <- sum(!is.na(historical_matches$target_cik))
n_bulk <- sum(!is.na(bulk_matches$target_cik))
improvement <- n_historical - n_bulk
pct_improvement <- round(100 * improvement / n_bulk, 1)

cat("RESULTS:\n")
cat(glue::glue("  Starting sample: {n_total} deals"), "\n")
cat(glue::glue("  Bulk matching: {n_bulk} ({round(100*n_bulk/n_total,1)}%)"), "\n")
cat(glue::glue("  Historical matching: {n_historical} ({round(100*n_historical/n_total,1)}%)"), "\n")
cat(glue::glue("  Improvement: +{improvement} deals (+{pct_improvement}%)"), "\n")
cat("\n")

cat("OUTPUT FILES:\n")
cat("  data/interim/deals_with_cik_historical.rds\n")
cat("  data/interim/historical_ticker_cik_database.rds\n")
cat("  data/interim/historical_matching_report.txt\n")
cat("\n")

cat("NEXT STEPS:\n")
cat("  1. Review: data/interim/historical_matching_report.txt\n")
cat("  2. Validate: Check match quality and name similarities\n")
cat("  3. Proceed: Rerun filing identification with improved matches\n")
cat("\n")

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

# Print recommendation
if (n_historical >= n_bulk * 3) {
  cat("✓ EXCELLENT: Historical matching tripled your CIK coverage!\n")
  cat("  Safe to proceed with filing identification.\n\n")
} else if (n_historical >= n_bulk * 2) {
  cat("✓ GOOD: Historical matching doubled your CIK coverage.\n")
  cat("  Review report and proceed with filing identification.\n\n")
} else if (n_historical > n_bulk * 1.5) {
  cat("✓ MODERATE: Historical matching improved coverage by 50%+.\n")
  cat("  Review report carefully before proceeding.\n\n")
} else {
  cat("⚠ LIMITED: Historical matching showed modest improvement.\n")
  cat("  Review report and consider additional strategies.\n\n")
}

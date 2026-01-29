# =============================================================================
# Test script for read_deals_v2_refined.R
# =============================================================================
# Purpose: Validate that the refined ingestion script produces expected output
#
# Usage: Rscript tests/test_ingestion_refined.R
# =============================================================================

cat("\n")
cat("=======================================================================\n")
cat("TESTING REFINED DEAL INGESTION SCRIPT\n")
cat("=======================================================================\n\n")

# Check if required packages are available
required_packages <- c("dplyr", "readr", "glue", "fs")
missing_packages <- required_packages[!sapply(required_packages, requireNamespace, quietly = TRUE)]

if (length(missing_packages) > 0) {
  stop(glue::glue("Missing required packages: {paste(missing_packages, collapse = ', ')}"))
}

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(glue)
  library(fs)
})

# =============================================================================
# TEST 1: Check that output file exists
# =============================================================================

cat("TEST 1: Checking output file exists...\n")

output_file <- "data/interim/deals_ingested.rds"
if (!fs::file_exists(output_file)) {
  cat("  ✗ FAIL: Output file not found\n")
  cat("  Run the ingestion script first: Rscript src/10_ingest/read_deals_v2_refined.R\n\n")
  stop("Output file missing")
}

cat("  ✓ PASS: Output file exists\n\n")

# =============================================================================
# TEST 2: Load dataset and check structure
# =============================================================================

cat("TEST 2: Loading dataset and checking structure...\n")

df <- tryCatch({
  readRDS(output_file)
}, error = function(e) {
  cat(glue("  ✗ FAIL: Could not load RDS file: {e$message}\n\n"))
  stop("Loading failed")
})

n_rows <- nrow(df)
n_cols <- ncol(df)

cat(glue("  Dataset dimensions: {n_rows} rows × {n_cols} columns\n"))

# Check expected row count (should be ~10,000 after deduplication)
if (n_rows < 5000 | n_rows > 20000) {
  cat(glue("  ⚠ WARNING: Row count {n_rows} outside expected range [5000, 20000]\n"))
} else {
  cat(glue("  ✓ Row count {n_rows} is reasonable\n"))
}

cat("\n")

# =============================================================================
# TEST 3: Check required variables exist
# =============================================================================

cat("TEST 3: Checking required variables exist...\n")

required_vars <- c(
  # Core identifiers
  "deal_id", "target_name", "target_ticker",
  # Dates
  "announce_date", "complete_date", "withdrawn_date",
  # Outcomes (derived)
  "deal_outcome_terminal", "deal_completed", "time_to_close",
  # Industry
  "target_sic_primary", "industry_broad",
  # Payment
  "payment_method_clean",
  # Fixed effects
  "year_announced", "industry_year",
  # Sample filter flags
  "flag_terminal_outcome", "flag_has_announce_date", 
  "flag_control_transfer", "flag_has_offer_price"
)

missing_vars <- setdiff(required_vars, names(df))

if (length(missing_vars) > 0) {
  cat(glue("  ✗ FAIL: Missing required variables: {paste(missing_vars, collapse = ', ')}\n\n"))
  stop("Required variables missing")
}

cat(glue("  ✓ PASS: All {length(required_vars)} required variables present\n\n"))

# =============================================================================
# TEST 4: Validate outcome variable values
# =============================================================================

cat("TEST 4: Validating outcome variable values...\n")

# Check deal_completed is binary (0, 1, or NA)
valid_completed_values <- c(0, 1, NA_integer_)
invalid_completed <- df %>%
  filter(!deal_completed %in% valid_completed_values) %>%
  nrow()

if (invalid_completed > 0) {
  cat(glue("  ✗ FAIL: {invalid_completed} rows have invalid deal_completed values\n\n"))
  stop("Invalid outcome values")
}

cat("  ✓ deal_completed is properly binary (0, 1, or NA)\n")

# Check time_to_close is non-negative where non-NA
negative_time <- df %>%
  filter(!is.na(time_to_close), time_to_close < 0) %>%
  nrow()

if (negative_time > 0) {
  cat(glue("  ⚠ WARNING: {negative_time} rows have negative time_to_close\n"))
} else {
  cat("  ✓ time_to_close is non-negative for all non-NA values\n")
}

# Check deal_outcome_terminal has expected categories
expected_outcomes <- c("completed", "withdrawn", "completed_assumed", "other")
outcome_levels <- unique(df$deal_outcome_terminal)
unexpected_outcomes <- setdiff(outcome_levels, expected_outcomes)

if (length(unexpected_outcomes) > 0) {
  cat(glue("  ⚠ WARNING: Unexpected outcome categories: {paste(unexpected_outcomes, collapse = ', ')}\n"))
} else {
  cat("  ✓ deal_outcome_terminal has expected categories\n")
}

cat("\n")

# =============================================================================
# TEST 5: Check sample filter flag distributions
# =============================================================================

cat("TEST 5: Checking sample filter flag distributions...\n")

flag_summary <- df %>%
  summarise(
    terminal_outcome = sum(flag_terminal_outcome, na.rm = TRUE),
    has_announce_date = sum(flag_has_announce_date, na.rm = TRUE),
    control_transfer = sum(flag_control_transfer, na.rm = TRUE),
    has_offer_price = sum(flag_has_offer_price, na.rm = TRUE)
  )

cat(glue("  Terminal outcome: {flag_summary$terminal_outcome} ({round(100*flag_summary$terminal_outcome/n_rows,1)}%)"), "\n")
cat(glue("  Has announce date: {flag_summary$has_announce_date} ({round(100*flag_summary$has_announce_date/n_rows,1)}%)"), "\n")
cat(glue("  Control transfer: {flag_summary$control_transfer} ({round(100*flag_summary$control_transfer/n_rows,1)}%)"), "\n")
cat(glue("  Has offer price: {flag_summary$has_offer_price} ({round(100*flag_summary$has_offer_price/n_rows,1)}%)"), "\n")

# Check that terminal outcome rate is reasonable (should be ~70-90%)
terminal_pct <- 100 * flag_summary$terminal_outcome / n_rows
if (terminal_pct < 50 | terminal_pct > 95) {
  cat(glue("  ⚠ WARNING: Terminal outcome rate {round(terminal_pct,1)}% outside expected range [50%, 95%]\n"))
} else {
  cat("  ✓ Terminal outcome rate is reasonable\n")
}

cat("\n")

# =============================================================================
# TEST 6: Check documentation files exist
# =============================================================================

cat("TEST 6: Checking documentation files exist...\n")

doc_files <- c(
  "data/interim/deals_ingestion_summary.txt",
  "data/interim/deals_ingestion_summary.json",
  "data/interim/deals_column_mapping.csv",
  "data/interim/deals_ingestion_log.txt"
)

missing_docs <- doc_files[!sapply(doc_files, fs::file_exists)]

if (length(missing_docs) > 0) {
  cat(glue("  ⚠ WARNING: Missing documentation files:\n"))
  for (f in missing_docs) {
    cat(glue("    - {f}\n"))
  }
} else {
  cat(glue("  ✓ PASS: All {length(doc_files)} documentation files present\n"))
}

cat("\n")

# =============================================================================
# TEST 7: Spot check data quality
# =============================================================================

cat("TEST 7: Spot checking data quality...\n")

# Check for reasonable completion rate
completion_rate <- mean(df$deal_completed == 1, na.rm = TRUE)
cat(glue("  Completion rate: {round(100*completion_rate,1)}%"), "\n")

if (completion_rate < 0.5 | completion_rate > 0.95) {
  cat("  ⚠ WARNING: Completion rate outside typical range [50%, 95%]\n")
} else {
  cat("  ✓ Completion rate is typical for M&A data\n")
}

# Check for reasonable time-to-close distribution
median_ttc <- median(df$time_to_close, na.rm = TRUE)
cat(glue("  Median time-to-close: {median_ttc} days"), "\n")

if (median_ttc < 30 | median_ttc > 500) {
  cat("  ⚠ WARNING: Median time-to-close outside typical range [30, 500] days\n")
} else {
  cat("  ✓ Median time-to-close is reasonable\n")
}

cat("\n")

# =============================================================================
# SUMMARY
# =============================================================================

cat("=======================================================================\n")
cat("TEST SUMMARY\n")
cat("=======================================================================\n\n")

cat("✓ All critical tests passed\n")
cat(glue("✓ Dataset ready for downstream processing: {n_rows} deals\n"))
cat("\n")
cat("Outcome distribution:\n")

outcome_counts <- df %>%
  count(deal_outcome_terminal) %>%
  mutate(pct = round(100 * n / sum(n), 1)) %>%
  arrange(desc(n))

for (i in seq_len(nrow(outcome_counts))) {
  row <- outcome_counts[i, ]
  cat(glue("  {row$deal_outcome_terminal}: {row$n} ({row$pct}%)"), "\n")
}

cat("\n")
cat("Next steps:\n")
cat("  1. Review data/interim/deals_ingestion_summary.txt for full statistics\n")
cat("  2. Proceed to sample restriction script (to be developed)\n")
cat("  3. Implement episode collapsing for competing bids (to be developed)\n")
cat("\n")
cat("=======================================================================\n")

cat("\nAll tests completed successfully!\n\n")

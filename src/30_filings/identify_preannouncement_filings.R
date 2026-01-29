# =============================================================================
# identify_preannouncement_filings.R
# =============================================================================
# Main script: Identify pre-announcement 10-K filings for M&A deals
#
# Purpose:
#   For each deal with a verified CIK identifier, query EDGAR to retrieve the
#   company's complete filing history, then apply research design timing rules
#   to select the single 10-K filing that best represents pre-deal disclosure.
#
# Research Design Logic:
#   - Filing must be a 10-K (original, not amendment)
#   - Filing must predate deal announcement
#   - Filing must be at least MINIMUM_LAG_DAYS before announcement
#   - Among qualifying filings, select the most recent
#
# Input:
#   data/interim/deals_with_cik_verified.rds (1,238 deals with CIK identifiers)
#
# Output:
#   data/interim/deals_with_filings.rds (deals matched to specific 10-K filings)
#   data/interim/filing_identification_log.txt (detailed processing log)
#   data/interim/filing_identification_summary.json (match statistics)
#   output/filing_identification_diagnostics.pdf (validation plots)
#
# Usage:
#   source("src/30_filings/identify_preannouncement_filings.R")
#
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(lubridate)
  library(ggplot2)
  library(glue)
  library(jsonlite)
})

# Source helper modules
source("src/30_filings/query_edgar_submissions.R")
source("src/30_filings/select_qualifying_filing.R")

# =============================================================================
# CONFIGURATION
# =============================================================================

# Input file
INPUT_FILE <- "data/interim/deals_with_cik_verified.rds"

# Output files
OUTPUT_FILE <- "data/interim/deals_with_filings.rds"
LOG_FILE <- "data/interim/filing_identification_log.txt"
SUMMARY_FILE <- "data/interim/filing_identification_summary.json"
DIAGNOSTICS_FILE <- "output/filing_identification_diagnostics.pdf"

# EDGAR query settings
EDGAR_CACHE_DIR <- "data/interim/edgar_submissions_cache"
USER_AGENT <- "mna-disclosure-research giuseppe.lando@studbocconi.it"

# Research design parameters (Literature-Aligned)
# Standard: 30 days minimum lag (conservative availability buffer)
# Rationale: Ensures filing publicly available and processable by acquirers
# Literature: Most studies use 10-30 days; 30 is conservative standard
MINIMUM_LAG_DAYS <- 30  # Literature standard for availability buffer
ALLOW_AMENDMENTS <- FALSE  # Use only original 10-K, not 10-K/A
ENABLE_FALLBACK <- TRUE  # Use prior 10-K when most recent too close (literature practice)

# =============================================================================
# SETUP
# =============================================================================

# Ensure output directories exist
dir.create(dirname(OUTPUT_FILE), recursive = TRUE, showWarnings = FALSE)
dir.create(dirname(DIAGNOSTICS_FILE), recursive = TRUE, showWarnings = FALSE)
dir.create(EDGAR_CACHE_DIR, recursive = TRUE, showWarnings = FALSE)

# Set up logging
log_conn <- file(LOG_FILE, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  message(full_msg)
}

log_msg("=================================================================", "INFO")
log_msg("FILING IDENTIFICATION: Pre-Announcement 10-K Selection", "INFO")
log_msg("=================================================================", "INFO")
log_msg(glue("Input: {INPUT_FILE}"), "INFO")
log_msg(glue("Output: {OUTPUT_FILE}"), "INFO")
log_msg(glue("Minimum lag: {MINIMUM_LAG_DAYS} days (literature standard)"), "INFO")
log_msg(glue("Allow amendments: {ALLOW_AMENDMENTS}"), "INFO")
log_msg(glue("Enable fallback to prior 10-K: {ENABLE_FALLBACK}"), "INFO")
log_msg(glue("Start time: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"), "INFO")

# =============================================================================
# LOAD DEALS WITH CIK IDENTIFIERS
# =============================================================================

log_msg("", "INFO")
log_msg("Loading deals with verified CIK identifiers...", "INFO")

if (!file.exists(INPUT_FILE)) {
  log_msg(glue("Input file not found: {INPUT_FILE}"), "ERROR")
  stop("Input file missing. Run revert_to_verified_matches.R first.")
}

deals <- readRDS(INPUT_FILE)

# Filter to deals with CIK matches only
deals_with_cik <- deals %>%
  filter(!is.na(target_cik))

n_total <- nrow(deals)
n_with_cik <- nrow(deals_with_cik)

log_msg(glue("Total deals loaded: {n_total}"), "INFO")
log_msg(glue("Deals with CIK: {n_with_cik}"), "INFO")

if (n_with_cik == 0) {
  log_msg("No deals with CIK identifiers found", "ERROR")
  stop("Cannot proceed without CIK-matched deals")
}

# Extract unique CIKs to query
unique_ciks <- deals_with_cik %>%
  pull(target_cik) %>%
  unique()

n_unique_ciks <- length(unique_ciks)

log_msg(glue("Unique CIKs to query: {n_unique_ciks}"), "INFO")

# =============================================================================
# QUERY EDGAR FOR FILING HISTORIES
# =============================================================================

log_msg("", "INFO")
log_msg("Querying EDGAR for company filing histories...", "INFO")
log_msg("This may take several minutes depending on cache status...", "INFO")

query_start <- Sys.time()

edgar_results <- batch_query_submissions(
  ciks = unique_ciks,
  cache_dir = EDGAR_CACHE_DIR,
  user_agent = USER_AGENT,
  verbose = FALSE,
  show_progress = TRUE
)

query_end <- Sys.time()
query_duration <- as.numeric(difftime(query_end, query_start, units = "secs"))

log_msg("", "INFO")
log_msg(glue("EDGAR query complete in {round(query_duration, 1)} seconds"), "INFO")
log_msg(glue("Successfully queried: {edgar_results$stats$n_successful} CIKs ({edgar_results$stats$pct_successful}%)"), "INFO")
log_msg(glue("Failed queries: {edgar_results$stats$n_failed} CIKs"), "INFO")

if (edgar_results$stats$n_failed > 0) {
  log_msg("Failed CIKs (first 10):", "DEBUG")
  failed_sample <- head(edgar_results$stats$failed_ciks, 10)
  for (cik in failed_sample) {
    log_msg(glue("  {cik}"), "DEBUG")
  }
}

all_filings <- edgar_results$filings

if (nrow(all_filings) == 0) {
  log_msg("No filing data retrieved from EDGAR", "ERROR")
  stop("Cannot proceed without filing data")
}

log_msg(glue("Total filings retrieved: {nrow(all_filings)}"), "INFO")

# Count 10-K filings specifically
n_10k_filings <- all_filings %>%
  filter(grepl("^10-K$", form_type, ignore.case = TRUE)) %>%
  nrow()

log_msg(glue("10-K filings available: {n_10k_filings}"), "INFO")

# =============================================================================
# SELECT QUALIFYING FILINGS FOR EACH DEAL
# =============================================================================

log_msg("", "INFO")
log_msg("Applying research design timing rules to select filings...", "INFO")

selection_start <- Sys.time()

deals_with_filings <- batch_select_filings(
  deals = deals_with_cik,
  all_filings = all_filings,
  minimum_lag_days = MINIMUM_LAG_DAYS,
  allow_amendments = ALLOW_AMENDMENTS,
  enable_fallback = ENABLE_FALLBACK
)

selection_end <- Sys.time()
selection_duration <- as.numeric(difftime(selection_end, selection_start, units = "secs"))

log_msg(glue("Filing selection complete in {round(selection_duration, 1)} seconds"), "INFO")

# =============================================================================
# MATCH STATISTICS
# =============================================================================

log_msg("", "INFO")
log_msg("Computing match statistics...", "INFO")

# Overall match rate
n_matched_primary <- sum(deals_with_filings$filing_selection_status == "matched_primary", na.rm = TRUE)
n_matched_fallback <- sum(deals_with_filings$filing_selection_status == "matched_fallback", na.rm = TRUE)
n_matched <- n_matched_primary + n_matched_fallback
n_unmatched <- n_with_cik - n_matched
pct_matched <- round(100 * n_matched / n_with_cik, 1)

log_msg("", "INFO")
log_msg("MATCH STATISTICS:", "INFO")
log_msg(glue("  Deals with CIK: {n_with_cik}"), "INFO")
log_msg(glue("  Successfully matched to 10-K: {n_matched} ({pct_matched}%)"), "INFO")
log_msg(glue("    Primary (most recent): {n_matched_primary}"), "INFO")
log_msg(glue("    Fallback (prior year): {n_matched_fallback}"), "INFO")
log_msg(glue("  Unmatched: {n_unmatched}"), "INFO")

# Breakdown by status
status_summary <- deals_with_filings %>%
  count(filing_selection_status) %>%
  mutate(pct = round(100 * n / n_with_cik, 1)) %>%
  arrange(desc(n))

log_msg("", "INFO")
log_msg("Match status breakdown:", "INFO")
for (i in seq_len(nrow(status_summary))) {
  row <- status_summary[i, ]
  log_msg(glue("  {row$filing_selection_status}: {row$n} ({row$pct}%)"), "INFO")
}

# Days before announcement distribution (for matched deals)
matched_deals <- deals_with_filings %>%
  filter(filing_selection_status %in% c("matched_primary", "matched_fallback"))

if (nrow(matched_deals) > 0) {
  days_stats <- matched_deals %>%
    summarize(
      min_days = min(days_before_announcement, na.rm = TRUE),
      q25 = quantile(days_before_announcement, 0.25, na.rm = TRUE),
      median = median(days_before_announcement, na.rm = TRUE),
      q75 = quantile(days_before_announcement, 0.75, na.rm = TRUE),
      max_days = max(days_before_announcement, na.rm = TRUE),
      mean_days = round(mean(days_before_announcement, na.rm = TRUE), 1)
    )
  
  log_msg("", "INFO")
  log_msg("Days between filing and announcement (matched deals):", "INFO")
  log_msg(glue("  Min: {days_stats$min_days} days"), "INFO")
  log_msg(glue("  25th percentile: {days_stats$q25} days"), "INFO")
  log_msg(glue("  Median: {days_stats$median} days"), "INFO")
  log_msg(glue("  75th percentile: {days_stats$q75} days"), "INFO")
  log_msg(glue("  Max: {days_stats$max_days} days"), "INFO")
  log_msg(glue("  Mean: {days_stats$mean_days} days"), "INFO")
}

# =============================================================================
# SAVE OUTPUTS
# =============================================================================

log_msg("", "INFO")
log_msg("Saving outputs...", "INFO")

# Save main dataset
saveRDS(deals_with_filings, OUTPUT_FILE)
log_msg(glue("✓ Saved: {OUTPUT_FILE}"), "INFO")

# Create summary JSON
summary_data <- list(
  input_file = INPUT_FILE,
  output_file = OUTPUT_FILE,
  processing_date = as.character(Sys.time()),
  configuration = list(
    minimum_lag_days = MINIMUM_LAG_DAYS,
    allow_amendments = ALLOW_AMENDMENTS,
    enable_fallback = ENABLE_FALLBACK
  ),
  input_counts = list(
    n_total_deals = n_total,
    n_with_cik = n_with_cik,
    n_unique_ciks = n_unique_ciks
  ),
  edgar_query_stats = edgar_results$stats,
  filing_counts = list(
    n_total_filings = nrow(all_filings),
    n_10k_filings = n_10k_filings
  ),
  match_statistics = list(
    n_matched = n_matched,
    n_matched_primary = n_matched_primary,
    n_matched_fallback = n_matched_fallback,
    n_unmatched = n_unmatched,
    pct_matched = pct_matched,
    status_breakdown = status_summary
  ),
  timing_statistics = if(nrow(matched_deals) > 0) days_stats else NULL
)

jsonlite::write_json(
  summary_data,
  SUMMARY_FILE,
  pretty = TRUE,
  auto_unbox = TRUE
)
log_msg(glue("✓ Saved: {SUMMARY_FILE}"), "INFO")

# =============================================================================
# GENERATE DIAGNOSTIC PLOTS
# =============================================================================

log_msg("", "INFO")
log_msg("Generating diagnostic visualizations...", "INFO")

pdf(DIAGNOSTICS_FILE, width = 11, height = 8.5)

# Plot 1: Match status distribution
p1 <- ggplot(status_summary, aes(x = reorder(filing_selection_status, -n), y = n)) +
  geom_col(fill = "steelblue") +
  geom_text(aes(label = glue("{n}\n({pct}%)")), vjust = -0.5, size = 3) +
  labs(
    title = "Filing Match Status Distribution",
    subtitle = glue("{n_with_cik} deals with CIK identifiers"),
    x = "Match Status",
    y = "Number of Deals"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

print(p1)

# Plot 2: Days before announcement distribution (if matches exist)
if (nrow(matched_deals) > 0) {
  p2 <- ggplot(matched_deals, aes(x = days_before_announcement)) +
    geom_histogram(bins = 50, fill = "steelblue", alpha = 0.7) +
    geom_vline(xintercept = MINIMUM_LAG_DAYS, color = "red", linetype = "dashed", size = 1) +
    annotate("text", x = MINIMUM_LAG_DAYS, y = Inf, 
             label = glue("Minimum lag\n({MINIMUM_LAG_DAYS} days)"), 
             vjust = 2, hjust = -0.1, color = "red") +
    labs(
      title = "Distribution of Days Between Filing and Announcement",
      subtitle = glue("{n_matched} matched deals"),
      x = "Days Before Announcement",
      y = "Number of Deals"
    ) +
    theme_minimal()
  
  print(p2)
  
  # Plot 3: Filing date vs announcement date
  p3 <- ggplot(matched_deals, aes(x = announce_date, y = filing_date)) +
    geom_point(alpha = 0.5, color = "steelblue") +
    geom_abline(slope = 1, intercept = 0, linetype = "dashed", color = "gray50") +
    geom_abline(slope = 1, intercept = -MINIMUM_LAG_DAYS, linetype = "dashed", color = "red") +
    annotate("text", x = min(matched_deals$announce_date, na.rm = TRUE), 
             y = max(matched_deals$filing_date, na.rm = TRUE),
             label = glue("{MINIMUM_LAG_DAYS}-day lag line"), color = "red", hjust = 0, vjust = 1) +
    labs(
      title = "Filing Date vs. Announcement Date",
      subtitle = "All points should fall below the red line (minimum lag requirement)",
      x = "Deal Announcement Date",
      y = "10-K Filing Date"
    ) +
    theme_minimal()
  
  print(p3)
  
  # Plot 4: Temporal distribution
  p4 <- matched_deals %>%
    mutate(year = year(announce_date)) %>%
    count(year) %>%
    ggplot(aes(x = year, y = n)) +
    geom_col(fill = "steelblue") +
    geom_text(aes(label = n), vjust = -0.5, size = 3) +
    labs(
      title = "Matched Deals by Announcement Year",
      subtitle = glue("{n_matched} deals with filing matches"),
      x = "Year",
      y = "Number of Matched Deals"
    ) +
    theme_minimal()
  
  print(p4)
}

dev.off()

log_msg(glue("✓ Saved: {DIAGNOSTICS_FILE}"), "INFO")

# =============================================================================
# COMPLETION
# =============================================================================

log_msg("", "INFO")
log_msg("=================================================================", "INFO")
log_msg("FILING IDENTIFICATION COMPLETE", "INFO")
log_msg("=================================================================", "INFO")
log_msg(glue("Total deals processed: {n_with_cik}"), "INFO")
log_msg(glue("Successfully matched: {n_matched} ({pct_matched}%)"), "INFO")
log_msg(glue("Unmatched: {n_unmatched}"), "INFO")
log_msg(glue("Output saved: {OUTPUT_FILE}"), "INFO")
log_msg(glue("End time: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"), "INFO")

close(log_conn)

# Print summary to console
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("FILING IDENTIFICATION COMPLETE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat(glue("Input:  {INPUT_FILE}"), "\n")
cat(glue("Output: {OUTPUT_FILE}"), "\n")
cat(glue("Deals with CIK: {n_with_cik}"), "\n")
cat(glue("Matched to 10-K: {n_matched} ({pct_matched}%)"), "\n")
cat(glue("  Primary selection: {n_matched_primary}"), "\n")
cat(glue("  Fallback to prior: {n_matched_fallback}"), "\n\n")

cat("Match status breakdown:\n")
for (i in seq_len(nrow(status_summary))) {
  row <- status_summary[i, ]
  cat(glue("  {row$filing_selection_status}: {row$n} deals ({row$pct}%)"), "\n")
}

if (nrow(matched_deals) > 0) {
  cat("\n")
  cat(glue("Filing timing: {days_stats$median} days median lag (range: {days_stats$min_days}-{days_stats$max_days})"), "\n")
}

cat("\n")
cat(glue("See {DIAGNOSTICS_FILE} for validation plots"), "\n")
cat(glue("See {LOG_FILE} for detailed processing log"), "\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

cat("Next step: Text extraction (download and parse selected 10-K filings)\n\n")

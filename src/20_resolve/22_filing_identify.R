# =============================================================================
# src/20_resolve/22_filing_identify.R
# =============================================================================
# STEP 3: Identify Pre-Announcement 10-K Filings
#
# For each deal with CIK, identifies the most recent 10-K filed before
# the announcement date, subject to research design timing constraints.
#
# Research Design Constraints:
#   - Filing must be BEFORE announcement date
#   - Minimum lag (default 30 days) to avoid contamination
#   - Prefer 10-K over 10-K/A (amended)
#   - Fallback to prior fiscal year if no qualifying filing
#
# Input:  data/interim/deals_with_cik.rds
# Output: data/interim/deals_with_filing.rds
#         data/interim/filing_identify_log.txt
#         data/interim/filing_selection_details.csv
#
# Usage:  source("src/20_resolve/22_filing_identify.R")
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(glue)
  library(jsonlite)
  library(httr)
  library(lubridate)
})

# =============================================================================
# CONFIGURATION (from config/filing_params.yaml logic)
# =============================================================================

CONFIG <- list(
  # SEC API
  sec_user_agent = "Giuseppe Lando <giuseppe.lando@studbocconi.it> ; M&A Disclosure Thesis",
  sec_submissions_url = "https://data.sec.gov/submissions/CIK{cik}.json",
  rate_limit_delay = 0.15,  # seconds between API calls
  
  # Filing selection parameters
  min_lag_days = 30,           # Minimum days between filing and announcement
  max_lookback_days = 450,     # Maximum days to look back (allow >1 year for fallback)
  
  # Form types (priority order)
  target_forms = c("10-K", "10-K/A", "20-F", "20-F/A"),
  prefer_original = TRUE,      # Prefer 10-K over 10-K/A if both qualify
  
  # Cache settings
  cache_dir = "data/interim/submissions_cache",
  
  # Fallback behavior
  allow_prior_year_fallback = TRUE
)

# =============================================================================
# SETUP
# =============================================================================

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("STEP 3: IDENTIFY PRE-ANNOUNCEMENT FILINGS\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

# Create cache directory
dir.create(CONFIG$cache_dir, recursive = TRUE, showWarnings = FALSE)

# Logging
log_file <- "data/interim/filing_identify_log.txt"
log_conn <- file(log_file, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  cat(full_msg, "\n")
}

log_msg("Filing identification process started")
log_msg(glue("Min lag: {CONFIG$min_lag_days} days"))
log_msg(glue("Max lookback: {CONFIG$max_lookback_days} days"))
log_msg(glue("Target forms: {paste(CONFIG$target_forms, collapse = ', ')}"))

# =============================================================================
# LOAD INPUT DATA
# =============================================================================

input_file <- "data/interim/deals_with_cik.rds"

if (!file.exists(input_file)) {
  log_msg("ERROR: Input file not found!", "ERROR")
  log_msg("Run CIK resolution first: source('src/20_resolve/21_cik_resolve.R')", "ERROR")
  close(log_conn)
  stop("Missing input: ", input_file)
}

deals <- readRDS(input_file)
n_total <- nrow(deals)
n_with_cik <- sum(!is.na(deals$target_cik))

log_msg(glue("Loaded {n_total} deals ({n_with_cik} with CIK)"))

# Initialize filing columns
deals <- deals %>%
  mutate(
    filing_accession = NA_character_,
    filing_date = as.Date(NA),
    filing_form = NA_character_,
    filing_primary_doc = NA_character_,
    filing_url = NA_character_,
    days_before_announcement = NA_integer_,
    filing_selection_reason = NA_character_
  )

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

# Format CIK for API (10 digits, zero-padded)
format_cik_api <- function(cik) {
  cik_num <- suppressWarnings(as.numeric(str_replace_all(cik, "^0+", "")))
  if (is.na(cik_num)) return(NA_character_)
  sprintf("%010d", cik_num)
}

# Get submissions from SEC (with caching)
get_sec_submissions <- function(cik) {
  cik_formatted <- format_cik_api(cik)
  if (is.na(cik_formatted)) return(NULL)
  
  cache_file <- file.path(CONFIG$cache_dir, glue("{cik_formatted}.json"))
  
  # Check cache
  if (file.exists(cache_file)) {
    cache_age <- difftime(Sys.time(), file.info(cache_file)$mtime, units = "days")
    if (cache_age < 30) {  # Use cache if < 30 days old
      tryCatch({
        return(fromJSON(cache_file))
      }, error = function(e) NULL)
    }
  }
  
  # Query API
  url <- str_replace(CONFIG$sec_submissions_url, "\\{cik\\}", cik_formatted)
  
  tryCatch({
    Sys.sleep(CONFIG$rate_limit_delay)
    
    resp <- GET(
      url,
      add_headers(`User-Agent` = CONFIG$sec_user_agent),
      timeout(30)
    )
    
    if (status_code(resp) == 200) {
      content_text <- content(resp, "text", encoding = "UTF-8")
      
      # Save to cache
      writeLines(content_text, cache_file)
      
      return(fromJSON(content_text))
    } else {
      return(NULL)
    }
  }, error = function(e) {
    return(NULL)
  })
}

# Extract filings from submissions JSON
extract_filings <- function(submissions_json) {
  if (is.null(submissions_json)) return(tibble())
  
  recent <- submissions_json$filings$recent
  if (is.null(recent) || length(recent$form) == 0) return(tibble())
  
  tibble(
    form = recent$form,
    filing_date = as.Date(recent$filingDate),
    accession = recent$accessionNumber,
    primary_doc = recent$primaryDocument,
    description = recent$primaryDocDescription
  )
}

# Select qualifying filing for a deal
select_filing <- function(filings, announce_date, min_lag, max_lookback) {
  if (nrow(filings) == 0) {
    return(list(
      accession = NA_character_,
      filing_date = as.Date(NA),
      form = NA_character_,
      primary_doc = NA_character_,
      days_before = NA_integer_,
      reason = "no_filings_found"
    ))
  }
  
  # Filter to target forms
  filings <- filings %>%
    filter(form %in% CONFIG$target_forms)
  
  if (nrow(filings) == 0) {
    return(list(
      accession = NA_character_,
      filing_date = as.Date(NA),
      form = NA_character_,
      primary_doc = NA_character_,
      days_before = NA_integer_,
      reason = "no_target_forms"
    ))
  }
  
  # Calculate days before announcement
  filings <- filings %>%
    mutate(
      days_before = as.integer(announce_date - filing_date)
    )
  
  # Apply timing constraints
  qualifying <- filings %>%
    filter(
      days_before >= min_lag,  # Must be at least min_lag days before
      days_before <= max_lookback  # Not too old
    )
  
  if (nrow(qualifying) == 0) {
    return(list(
      accession = NA_character_,
      filing_date = as.Date(NA),
      form = NA_character_,
      primary_doc = NA_character_,
      days_before = NA_integer_,
      reason = "no_qualifying_timing"
    ))
  }
  
  # Sort by preference: (1) original vs amended, (2) most recent
  if (CONFIG$prefer_original) {
    qualifying <- qualifying %>%
      mutate(
        is_amended = str_detect(form, "/A$"),
        priority = case_when(
          form %in% c("10-K", "20-F") ~ 1,
          form %in% c("10-K/A", "20-F/A") ~ 2,
          TRUE ~ 3
        )
      ) %>%
      arrange(priority, days_before)  # Lower priority number = better
  } else {
    qualifying <- qualifying %>%
      arrange(days_before)  # Most recent first
  }
  
  # Select best
  best <- qualifying[1, ]
  
  list(
    accession = best$accession,
    filing_date = best$filing_date,
    form = best$form,
    primary_doc = best$primary_doc,
    days_before = best$days_before,
    reason = "selected"
  )
}

# Build filing URL
build_filing_url <- function(cik, accession, primary_doc) {
  if (any(is.na(c(cik, accession, primary_doc)))) return(NA_character_)
  
  cik_clean <- str_replace_all(cik, "^0+", "")
  accession_clean <- str_replace_all(accession, "-", "")
  
  glue("https://www.sec.gov/Archives/edgar/data/{cik_clean}/{accession_clean}/{primary_doc}")
}

# =============================================================================
# MAIN PROCESSING LOOP
# =============================================================================

log_msg("")
log_msg("Processing deals with CIK...", "INFO")
log_msg(paste(rep("-", 50), collapse = ""))

# Get deals to process
deals_to_process <- deals %>%
  filter(!is.na(target_cik)) %>%
  select(deal_id, target_cik, target_ticker, target_name, date_announced)

n_to_process <- nrow(deals_to_process)
log_msg(glue("Processing {n_to_process} deals"))

# Track results
selection_details <- list()
n_found <- 0
n_not_found <- 0

# Progress tracking
progress_interval <- max(1, floor(n_to_process / 20))

for (i in seq_len(n_to_process)) {
  deal <- deals_to_process[i, ]
  
  # Progress
  if (i %% progress_interval == 0 || i == n_to_process) {
    pct <- round(100 * i / n_to_process)
    log_msg(glue("Progress: {i}/{n_to_process} ({pct}%) - Found: {n_found}, Missing: {n_not_found}"), "DEBUG")
  }
  
  # Get submissions
  submissions <- get_sec_submissions(deal$target_cik)
  
  if (is.null(submissions)) {
    selection_details[[i]] <- tibble(
      deal_id = deal$deal_id,
      target_cik = deal$target_cik,
      target_ticker = deal$target_ticker,
      announce_date = deal$date_announced,
      filing_accession = NA_character_,
      filing_date = as.Date(NA),
      filing_form = NA_character_,
      days_before = NA_integer_,
      selection_reason = "api_error"
    )
    n_not_found <- n_not_found + 1
    next
  }
  
  # Extract filings
  filings <- extract_filings(submissions)
  
  # Select qualifying filing
  result <- select_filing(
    filings,
    announce_date = deal$date_announced,
    min_lag = CONFIG$min_lag_days,
    max_lookback = CONFIG$max_lookback_days
  )
  
  # Build URL
  filing_url <- build_filing_url(deal$target_cik, result$accession, result$primary_doc)
  
  # Store result
  selection_details[[i]] <- tibble(
    deal_id = deal$deal_id,
    target_cik = deal$target_cik,
    target_ticker = deal$target_ticker,
    announce_date = deal$date_announced,
    filing_accession = result$accession,
    filing_date = result$filing_date,
    filing_form = result$form,
    filing_primary_doc = result$primary_doc,
    filing_url = filing_url,
    days_before = result$days_before,
    selection_reason = result$reason
  )
  
  if (!is.na(result$accession)) {
    n_found <- n_found + 1
  } else {
    n_not_found <- n_not_found + 1
  }
}

# Combine results
selection_df <- bind_rows(selection_details)

# =============================================================================
# UPDATE MAIN DATASET
# =============================================================================

log_msg("")
log_msg("Updating main dataset...", "INFO")

deals <- deals %>%
  left_join(
    selection_df %>%
      select(deal_id, filing_accession, filing_date, filing_form, 
             filing_primary_doc, filing_url, days_before, selection_reason),
    by = "deal_id"
  ) %>%
  mutate(
    filing_accession = coalesce(filing_accession.x, filing_accession.y),
    filing_date = coalesce(filing_date.x, filing_date.y),
    filing_form = coalesce(filing_form.x, filing_form.y),
    filing_primary_doc = coalesce(filing_primary_doc.x, filing_primary_doc.y),
    filing_url = coalesce(filing_url.x, filing_url.y),
    days_before_announcement = coalesce(days_before_announcement, days_before),
    filing_selection_reason = coalesce(filing_selection_reason, selection_reason)
  ) %>%
  select(-ends_with(".x"), -ends_with(".y"), -days_before, -selection_reason)

# =============================================================================
# SUMMARY AND DIAGNOSTICS
# =============================================================================

log_msg("")
log_msg(paste(rep("=", 70), collapse = ""))
log_msg("FILING IDENTIFICATION SUMMARY")
log_msg(paste(rep("=", 70), collapse = ""))

n_with_filing <- sum(!is.na(deals$filing_accession))
n_without_filing <- sum(is.na(deals$filing_accession) & !is.na(deals$target_cik))
filing_rate <- round(100 * n_with_filing / n_with_cik, 1)

log_msg(glue("Deals with CIK: {n_with_cik}"))
log_msg(glue("Filings found: {n_with_filing} ({filing_rate}%)"))
log_msg(glue("Filings not found: {n_without_filing} ({round(100 - filing_rate, 1)}%)"))

# Reason breakdown
reason_summary <- selection_df %>%
  count(selection_reason) %>%
  arrange(desc(n))

log_msg("")
log_msg("Selection reasons:")
for (i in seq_len(nrow(reason_summary))) {
  row <- reason_summary[i, ]
  pct <- round(100 * row$n / n_to_process, 1)
  log_msg(glue("  {row$selection_reason}: {row$n} ({pct}%)"))
}

# Form type distribution
form_summary <- selection_df %>%
  filter(!is.na(filing_form)) %>%
  count(filing_form) %>%
  arrange(desc(n))

log_msg("")
log_msg("Form types selected:")
for (i in seq_len(nrow(form_summary))) {
  row <- form_summary[i, ]
  pct <- round(100 * row$n / n_with_filing, 1)
  log_msg(glue("  {row$filing_form}: {row$n} ({pct}%)"))
}

# Lag statistics
if (n_with_filing > 0) {
  lag_stats <- selection_df %>%
    filter(!is.na(days_before)) %>%
    summarise(
      median_lag = median(days_before),
      mean_lag = round(mean(days_before), 1),
      min_lag = min(days_before),
      max_lag = max(days_before)
    )
  
  log_msg("")
  log_msg("Filing lag statistics (days before announcement):")
  log_msg(glue("  Median: {lag_stats$median_lag}"))
  log_msg(glue("  Mean: {lag_stats$mean_lag}"))
  log_msg(glue("  Range: {lag_stats$min_lag} - {lag_stats$max_lag}"))
}

# =============================================================================
# SAVE OUTPUTS
# =============================================================================

# Main output
output_file <- "data/interim/deals_with_filing.rds"
saveRDS(deals, output_file)
log_msg(glue("Saved: {output_file}"))

# Selection details
write.csv(selection_df, "data/interim/filing_selection_details.csv", row.names = FALSE)
log_msg("Saved: data/interim/filing_selection_details.csv")

# Deals ready for download (have filing)
deals_ready <- deals %>%
  filter(!is.na(filing_accession)) %>%
  select(deal_id, target_cik, target_ticker, target_name, date_announced,
         filing_accession, filing_date, filing_form, filing_url, 
         days_before_announcement)

write.csv(deals_ready, "data/interim/deals_ready_for_download.csv", row.names = FALSE)
log_msg(glue("Saved: data/interim/deals_ready_for_download.csv ({nrow(deals_ready)} deals)"))

# Summary JSON
summary_stats <- list(
  timestamp = as.character(Sys.time()),
  config = CONFIG[c("min_lag_days", "max_lookback_days", "target_forms")],
  input_file = input_file,
  output_file = output_file,
  n_total_deals = n_total,
  n_with_cik = n_with_cik,
  n_with_filing = n_with_filing,
  filing_rate_pct = filing_rate,
  by_reason = as.list(setNames(reason_summary$n, reason_summary$selection_reason)),
  by_form = as.list(setNames(form_summary$n, form_summary$filing_form))
)
write_json(summary_stats, "data/interim/filing_identify_summary.json", pretty = TRUE, auto_unbox = TRUE)

log_msg("")
log_msg("Filing identification complete")
close(log_conn)

# Console summary
cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║              FILING IDENTIFICATION COMPLETE                      ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Deals with CIK:    %-46d ║\n", n_with_cik))
cat(sprintf("║ Filings found:     %-46s ║\n", glue("{n_with_filing} ({filing_rate}%)")))
cat(sprintf("║ Ready for download: %-45d ║\n", nrow(deals_ready)))
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ Output: data/interim/deals_with_filing.rds                       ║\n")
cat("║         data/interim/deals_ready_for_download.csv                ║\n")
cat("║ Log:    data/interim/filing_identify_log.txt                     ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")
cat("\n")

# Overall pipeline summary
overall_rate <- round(100 * n_with_filing / n_total, 1)
cat(glue("PIPELINE SUMMARY: {n_total} deals → {n_with_cik} with CIK → {n_with_filing} with filing ({overall_rate}%)"), "\n")
cat("\n")

if (n_with_filing > 0) {
  cat("✓ Ready for filing download and text extraction!\n")
  cat("  Next: source('src/30_download/30_download_filings.R')\n\n")
} else {
  cat("⚠ No filings found. Check:\n")
  cat("  1. CIK resolution quality\n")
  cat("  2. Timing constraints (min_lag_days)\n")
  cat("  3. SEC API connectivity\n\n")
}

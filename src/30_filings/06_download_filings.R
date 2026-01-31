# =============================================================================
# src/10_ingest/get_filings_v2.R
# =============================================================================
# Config-driven SEC filing retrieval with comprehensive logging
#
# Features:
# - Multiple scenario support (baseline, sensitivity checks)
# - Detailed filing selection logic with audit trail
# - Comprehensive validation and error handling
# - Summary statistics generation
#
# Usage:
#   Rscript src/10_ingest/get_filings_v2.R [--scenario baseline] [--config config/filing_params.yaml]
#
# Output:
#   - data/raw/filings/{ticker}/... (HTML filings)
#   - data/interim/filing_selection_log.csv (detailed selection log)
#   - data/interim/filing_summary_stats.json (summary statistics)
#   - data/interim/filing_processing_log.csv (success/failure per deal)
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(lubridate)
  library(readr)
  library(stringr)
  library(httr)
  library(jsonlite)
  library(fs)
  library(glue)
  library(purrr)
  library(tidyr)
  library(yaml)
})

# =============================================================================
# CONFIGURATION
# =============================================================================

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)
scenario_name <- "baseline"  # default
config_path <- "config/filing_params.yaml"

for (i in seq_along(args)) {
  if (args[i] == "--scenario" && i < length(args)) {
    scenario_name <- args[i + 1]
  }
  if (args[i] == "--config" && i < length(args)) {
    config_path <- args[i + 1]
  }
}

# Load configuration
if (!file.exists(config_path)) {
  stop(glue("Config file not found: {config_path}"))
}

config <- yaml::read_yaml(config_path)

# Select scenario
if (!scenario_name %in% names(config$scenarios)) {
  stop(glue("Scenario '{scenario_name}' not found in config. Available: {paste(names(config$scenarios), collapse = ', ')}"))
}

scenario <- config$scenarios[[scenario_name]]

message("=== SEC Filing Retrieval ===")
message(glue("Scenario: {scenario_name}"))
message(glue("Description: {scenario$description}"))
message(glue("Filing window: {scenario$filing_window_days} days pre-announcement"))
message(glue("Form types: {paste(scenario$form_types, collapse = ', ')}"))
message("")

# =============================================================================
# INPUT DATA
# =============================================================================

deals_path <- "data/interim/deals_with_cik.csv"
if (!file.exists(deals_path)) {
  stop(glue("Input file not found: {deals_path}. Run map_tickers_to_cik.R first."))
}

deals_all <- readr::read_csv(deals_path, show_col_types = FALSE)

# Identify announce_date column
announce_col <- case_when(
  "announce_date" %in% names(deals_all) ~ "announce_date",
  "announced_date" %in% names(deals_all) ~ "announced_date",
  TRUE ~ NA_character_
)

if (is.na(announce_col)) {
  stop("Missing announcement date column. Expected 'announce_date' or 'announced_date'.")
}

# Standardize
deals_all <- deals_all %>%
  mutate(announce_date = as.Date(.data[[announce_col]]))

# Filter to deals with CIK and valid announce date
deals <- deals_all %>%
  filter(!is.na(cik), !is.na(announce_date)) %>%
  mutate(
    cik_pad = str_pad(as.character(cik), width = 10, side = "left", pad = "0"),
    cik_int = as.integer(cik)
  )

message(glue("Input: {nrow(deals_all)} total deals"))
message(glue("Processing: {nrow(deals)} deals with CIK + announce_date"))
message("")

# =============================================================================
# SETUP OUTPUT DIRECTORIES
# =============================================================================

fs::dir_create(config$caching$filings_cache_dir)
fs::dir_create(config$caching$submissions_cache_dir)
fs::dir_create(dirname(config$output$filing_log_path))

# Initialize logs
filing_log <- list()
processing_log <- list()

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

# SEC API headers
ua_header <- httr::add_headers(`User-Agent` = config$sec_api$user_agent)

# Get cached or fetch submissions JSON
fetch_submissions <- function(cik_pad) {
  cache_file <- file.path(config$caching$submissions_cache_dir, glue("CIK{cik_pad}.json"))
  
  if (config$caching$cache_submissions && file.exists(cache_file)) {
    txt <- readr::read_file(cache_file)
  } else {
    url <- glue("{config$sec_api$base_url}/submissions/CIK{cik_pad}.json")
    Sys.sleep(config$sec_api$rate_limit_delay_sec)
    
    resp <- httr::GET(url, ua_header)
    if (httr::http_error(resp)) {
      return(NULL)
    }
    
    txt <- httr::content(resp, "text", encoding = "UTF-8")
    
    if (config$caching$cache_submissions) {
      writeLines(txt, cache_file, useBytes = TRUE)
    }
  }
  
  jsonlite::fromJSON(txt)
}

# Construct document URL
make_doc_url <- function(cik_int, accession_no, primary_doc) {
  acc_clean <- gsub("-", "", accession_no)
  glue("https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/{primary_doc}")
}

# Check if date in valid window
in_window <- function(filing_date, announce_date, window_days) {
  !is.na(announce_date) && 
    !is.na(filing_date) &&
    filing_date >= (announce_date - window_days) &&
    filing_date < announce_date  # Strictly before announcement
}

# Prioritize filings by form type
get_priority <- function(form_type) {
  scenario$form_priority[[form_type]] %||% 0
}

# Download filing with retry logic
download_filing <- function(url, dest_path, max_retries = config$error_handling$max_retries) {
  for (attempt in 1:max_retries) {
    Sys.sleep(config$sec_api$rate_limit_delay_sec)
    
    result <- tryCatch({
      resp <- httr::GET(url, ua_header, httr::write_disk(dest_path, overwrite = TRUE))
      
      if (!httr::http_error(resp) && file.exists(dest_path)) {
        file_size <- file.info(dest_path)$size
        if (!is.na(file_size) && file_size >= scenario$min_file_size_bytes) {
          return(list(success = TRUE, size = file_size, attempt = attempt))
        }
      }
      
      list(success = FALSE, size = 0, attempt = attempt)
    }, error = function(e) {
      list(success = FALSE, size = 0, attempt = attempt, error = e$message)
    })
    
    if (result$success) {
      return(result)
    }
    
    if (attempt < max_retries) {
      Sys.sleep(config$error_handling$retry_delay_sec)
    }
  }
  
  return(result)
}

# =============================================================================
# MAIN PROCESSING LOOP
# =============================================================================

message("Starting filing retrieval...")
message("")

n_total <- nrow(deals)
pb <- txtProgressBar(min = 0, max = n_total, style = 3)

for (i in seq_len(n_total)) {
  setTxtProgressBar(pb, i)
  
  deal <- deals[i, ]
  
  # Initialize tracking
  found_count <- 0L
  downloaded_count <- 0L
  skip_reason <- NA_character_
  
  # Validate announce date
  ann_date <- as.Date(deal$announce_date)
  if (is.na(ann_date)) {
    skip_reason <- "missing_announce_date"
    processing_log[[length(processing_log) + 1]] <- tibble(
      deal_id = deal$deal_id %||% deal$deal_number %||% i,
      target_ticker = deal$target_ticker,
      cik = deal$cik,
      status = "skipped",
      reason = skip_reason,
      found = 0L,
      downloaded = 0L
    )
    next
  }
  
  # Fetch submissions
  subm <- fetch_submissions(deal$cik_pad)
  if (is.null(subm) || is.null(subm$filings$recent)) {
    skip_reason <- "no_submissions_json"
    processing_log[[length(processing_log) + 1]] <- tibble(
      deal_id = deal$deal_id %||% deal$deal_number %||% i,
      target_ticker = deal$target_ticker,
      cik = deal$cik,
      status = "failed",
      reason = skip_reason,
      found = 0L,
      downloaded = 0L
    )
    next
  }
  
  # Build filings dataframe
  recent <- subm$filings$recent
  filings_df <- tibble(
    form = recent$form,
    filing_date = as.Date(recent$filingDate),
    accession = recent$accessionNumber,
    primary = recent$primaryDocument
  ) %>%
    distinct(accession, .keep_all = TRUE) %>%
    filter(
      form %in% scenario$form_types,
      in_window(filing_date, ann_date, scenario$filing_window_days)
    ) %>%
    mutate(priority = map_dbl(form, get_priority)) %>%
    arrange(desc(priority), desc(filing_date))
  
  found_count <- nrow(filings_df)
  
  if (found_count == 0L) {
    skip_reason <- "no_filings_in_window"
    processing_log[[length(processing_log) + 1]] <- tibble(
      deal_id = deal$deal_id %||% deal$deal_number %||% i,
      target_ticker = deal$target_ticker,
      cik = deal$cik,
      status = "no_filings",
      reason = skip_reason,
      found = 0L,
      downloaded = 0L
    )
    next
  }
  
  # Cap number of filings
  filings_df <- filings_df %>%
    slice_head(n = scenario$max_docs_per_issuer)
  
  # Create ticker directory
  ticker <- deal$target_ticker %||% glue("CIK_{deal$cik}")
  ticker_dir <- file.path(config$caching$filings_cache_dir, ticker)
  fs::dir_create(ticker_dir)
  
  # Download filings
  for (j in seq_len(nrow(filings_df))) {
    filing <- filings_df[j, ]
    
    url <- make_doc_url(deal$cik_int, filing$accession, filing$primary)
    
    # Generate filename
    filename <- glue::glue_data(
      list(
        ticker = ticker,
        form = filing$form,
        filing_date = format(filing$filing_date, "%Y-%m-%d"),
        cik = deal$cik_pad,
        accession = gsub("-", "", filing$accession)
      ),
      config$output$filename_template
    ) %>%
      str_replace_all("[: /]", "-")
    
    dest_path <- file.path(ticker_dir, filename)
    
    # Download
    dl_result <- download_filing(url, dest_path)
    
    # Log this filing
    filing_log[[length(filing_log) + 1]] <- tibble(
      deal_id = deal$deal_id %||% deal$deal_number %||% i,
      target_ticker = deal$target_ticker,
      cik = deal$cik,
      announce_date = ann_date,
      form_type = filing$form,
      filing_date = filing$filing_date,
      accession_number = filing$accession,
      primary_document = filing$primary,
      file_url = url,
      local_path = if (dl_result$success) dest_path else NA_character_,
      file_size_bytes = dl_result$size,
      selected_reason = if (j == 1) "top_priority" else "additional",
      download_success = dl_result$success,
      download_attempts = dl_result$attempt,
      filing_lag_days = as.integer(ann_date - filing$filing_date),
      priority = filing$priority
    )
    
    if (dl_result$success) {
      downloaded_count <- downloaded_count + 1L
    }
  }
  
  # Log processing outcome
  processing_log[[length(processing_log) + 1]] <- tibble(
    deal_id = deal$deal_id %||% deal$deal_number %||% i,
    target_ticker = deal$target_ticker,
    cik = deal$cik,
    status = if (downloaded_count > 0) "success" else "download_failed",
    reason = if (downloaded_count > 0) "ok" else "all_downloads_failed",
    found = found_count,
    downloaded = downloaded_count
  )
}

close(pb)
message("\n")

# =============================================================================
# SAVE LOGS AND STATISTICS
# =============================================================================

message("Saving logs and statistics...")

# Filing selection log
if (length(filing_log) > 0) {
  filing_log_df <- bind_rows(filing_log)
  readr::write_csv(filing_log_df, config$output$filing_log_path)
  message(glue("✓ Filing selection log: {config$output$filing_log_path}"))
}

# Processing log
if (length(processing_log) > 0) {
  processing_log_df <- bind_rows(processing_log)
  readr::write_csv(processing_log_df, config$output$processing_log_path)
  message(glue("✓ Processing log: {config$output$processing_log_path}"))
  
  # Summary statistics
  summary_stats <- list(
    scenario = scenario_name,
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    total_deals = n_total,
    deals_with_filings = sum(processing_log_df$status == "success"),
    deals_without_filings = sum(processing_log_df$status != "success"),
    total_filings_found = sum(processing_log_df$found),
    total_filings_downloaded = sum(processing_log_df$downloaded),
    success_rate_pct = round(100 * mean(processing_log_df$status == "success"), 2)
  )
  
  # Add form type distribution if filing log exists
  if (length(filing_log) > 0) {
    form_dist <- filing_log_df %>%
      filter(download_success) %>%
      count(form_type) %>%
      deframe()
    
    summary_stats$form_type_distribution <- form_dist
    
    # Filing lag statistics
    summary_stats$median_filing_lag_days <- median(filing_log_df$filing_lag_days, na.rm = TRUE)
    summary_stats$mean_filing_lag_days <- round(mean(filing_log_df$filing_lag_days, na.rm = TRUE), 1)
    summary_stats$max_filing_lag_days <- max(filing_log_df$filing_lag_days, na.rm = TRUE)
    
    # Coverage by form type
    summary_stats$pct_with_10k <- round(100 * mean(grepl("10-K", processing_log_df$status)), 2)
    summary_stats$pct_with_10q <- round(100 * mean(grepl("10-Q", processing_log_df$status)), 2)
  }
  
  jsonlite::write_json(
    summary_stats,
    config$output$summary_stats_path,
    pretty = TRUE,
    auto_unbox = TRUE
  )
  message(glue("✓ Summary statistics: {config$output$summary_stats_path}"))
}

# =============================================================================
# PRINT SUMMARY
# =============================================================================

cat("\n")
cat("=" %>% rep(70) %>% paste(collapse = ""), "\n")
cat("SEC FILING RETRIEVAL SUMMARY\n")
cat("=" %>% rep(70) %>% paste(collapse = ""), "\n")
cat(glue("Scenario:         {scenario_name}"), "\n")
cat(glue("Total deals:      {n_total}"), "\n")
cat(glue("With filings:     {summary_stats$deals_with_filings} ({summary_stats$success_rate_pct}%)"), "\n")
cat(glue("Filings found:    {summary_stats$total_filings_found}"), "\n")
cat(glue("Downloaded:       {summary_stats$total_filings_downloaded}"), "\n")
if (!is.null(summary_stats$median_filing_lag_days)) {
  cat(glue("Median lag:       {summary_stats$median_filing_lag_days} days"), "\n")
}
cat("=" %>% rep(70) %>% paste(collapse = ""), "\n")

message("\nComplete!")

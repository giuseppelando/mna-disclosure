# =============================================================================
# select_qualifying_filing.R (REVISED - Literature-Aligned)
# =============================================================================
# Filing selection logic implementing M&A-disclosure literature standards
#
# Key Changes from Initial Version:
#   - Default minimum lag: 30 days (not 90) per literature consensus
#   - Fallback mechanism: Use prior 10-K when most recent is too close
#   - Sample preservation: Fewer exclusions via fallback logic
#   - Robustness ready: Easy to test 10/20/30 day specifications
#
# Literature Rationale:
#   The objective is to ensure textual measures reflect routine disclosure
#   produced outside the public deal window and plausibly used by acquirers
#   in screening. A 30-day buffer ensures availability without requiring stale
#   information. When most recent 10-K is too close, prior 10-K is preferred
#   over exclusion to preserve sample size while maintaining timing discipline.
#
# =============================================================================

suppressPackageStartupMessages({
  require(dplyr)
  require(lubridate)
  require(glue)
})

#' Select Qualifying 10-K Filing Using Literature-Standard Logic
#'
#' Implements M&A-disclosure literature conventions for selecting pre-announcement
#' 10-K filings with availability buffer and fallback mechanism.
#'
#' Selection Algorithm:
#'   1. Filter to 10-K forms only (original filings, not amendments)
#'   2. Identify most recent 10-K filed before announcement
#'   3. Check if it meets minimum lag requirement (default: 30 days)
#'   4. If yes: Use most recent 10-K (primary selection)
#'   5. If no: Use prior (second-most-recent) 10-K as fallback
#'   6. If no fallback available: Mark as unmatched
#'
#' @param filing_history Data frame with columns: form_type, filing_date, accession_number, filing_url
#' @param announcement_date Date, the deal announcement date
#' @param minimum_lag_days Integer, minimum days for availability (default: 30 per literature)
#' @param allow_amendments Logical, whether to allow 10-K/A (default: FALSE)
#' @param enable_fallback Logical, use prior 10-K when most recent too close (default: TRUE)
#'
#' @return List with elements:
#'   - status: "matched_primary", "matched_fallback", "no_10k_filings", "no_qualifying_filing"
#'   - filing: Single-row data frame with selected filing (NULL if no match)
#'   - selection_method: "most_recent" or "prior_year_fallback"
#'   - n_10k_filings: Total number of 10-K forms available
#'   - days_before_announcement: Gap between selected filing and announcement
#'   - cutoff_date: Minimum allowable filing date (announcement - lag)
#'   - most_recent_10k_date: Date of most recent 10-K (for diagnostics)
#'   - used_fallback: Logical, whether fallback was triggered
#'
#' @details
#' Literature Standard (30-day buffer + fallback):
#'   - Most studies use 10-30 days minimum lag (30 = conservative standard)
#'   - Purpose: Ensure filing publicly available, not strict causal identification
#'   - Fallback preserves sample size while maintaining timing discipline
#'   - This aligns with Loughran & McDonald, Hoberg & Phillips, etc.
#'
#' Edge Cases:
#'   - No 10-Ks exist: status = "no_10k_filings"
#'   - Only one 10-K and it's too close: status = "no_qualifying_filing"
#'   - Most recent too close but prior exists: status = "matched_fallback"
#'
#' @examples
#' # Example: Deal announced 2020-06-15, most recent 10-K on 2020-06-01 (14 days before)
#' # With 30-day minimum: Most recent excluded, use prior 10-K from 2019-03-01
#' result <- select_qualifying_filing(filing_history, as.Date("2020-06-15"))
#' # result$status = "matched_fallback"
#' # result$filing$filing_date = 2019-03-01 (prior year)
#' # result$used_fallback = TRUE
#'
select_qualifying_filing <- function(filing_history,
                                    announcement_date,
                                    minimum_lag_days = 30,  # Literature standard
                                    allow_amendments = FALSE,
                                    enable_fallback = TRUE) {
  
  # Validate inputs
  if (!inherits(announcement_date, "Date")) {
    stop("announcement_date must be a Date object")
  }
  
  if (!is.data.frame(filing_history) || nrow(filing_history) == 0) {
    return(list(
      status = "no_filings",
      filing = NULL,
      selection_method = NA_character_,
      n_10k_filings = 0,
      days_before_announcement = NA_integer_,
      cutoff_date = announcement_date - minimum_lag_days,
      most_recent_10k_date = as.Date(NA),
      used_fallback = FALSE
    ))
  }
  
  # Calculate cutoff date
  cutoff_date <- announcement_date - minimum_lag_days
  
  # Filter to 10-K forms
  form_type_pattern <- if (allow_amendments) "^10-K" else "^10-K$"
  
  ten_k_filings <- filing_history %>%
    filter(
      grepl(form_type_pattern, form_type, ignore.case = TRUE),
      filing_date < announcement_date  # Must predate announcement
    ) %>%
    arrange(desc(filing_date))  # Most recent first
  
  n_10k_total <- nrow(ten_k_filings)
  
  if (n_10k_total == 0) {
    return(list(
      status = "no_10k_filings",
      filing = NULL,
      selection_method = NA_character_,
      n_10k_filings = 0,
      days_before_announcement = NA_integer_,
      cutoff_date = cutoff_date,
      most_recent_10k_date = as.Date(NA),
      used_fallback = FALSE
    ))
  }
  
  # Get most recent 10-K
  most_recent_10k <- ten_k_filings[1, ]
  most_recent_date <- most_recent_10k$filing_date
  
  # Check if most recent meets minimum lag
  if (most_recent_date <= cutoff_date) {
    # Primary selection: Most recent 10-K is far enough before announcement
    days_before <- as.integer(announcement_date - most_recent_date)
    
    return(list(
      status = "matched_primary",
      filing = most_recent_10k,
      selection_method = "most_recent",
      n_10k_filings = n_10k_total,
      days_before_announcement = days_before,
      cutoff_date = cutoff_date,
      most_recent_10k_date = most_recent_date,
      used_fallback = FALSE
    ))
  }
  
  # Most recent is too close - try fallback to prior 10-K
  if (enable_fallback && n_10k_total >= 2) {
    # Get second-most-recent 10-K (prior fiscal year)
    prior_10k <- ten_k_filings[2, ]
    days_before <- as.integer(announcement_date - prior_10k$filing_date)
    
    return(list(
      status = "matched_fallback",
      filing = prior_10k,
      selection_method = "prior_year_fallback",
      n_10k_filings = n_10k_total,
      days_before_announcement = days_before,
      cutoff_date = cutoff_date,
      most_recent_10k_date = most_recent_date,
      used_fallback = TRUE
    ))
  }
  
  # No qualifying filing available
  return(list(
    status = "no_qualifying_filing",
    filing = NULL,
    selection_method = NA_character_,
    n_10k_filings = n_10k_total,
    days_before_announcement = NA_integer_,
    cutoff_date = cutoff_date,
    most_recent_10k_date = most_recent_date,
    used_fallback = FALSE
  ))
}


#' Batch Select Filings for Multiple Deals
#'
#' Apply filing selection logic to a dataset of deals with filing histories
#'
#' @param deals Data frame with columns: target_cik, announce_date
#' @param all_filings Data frame with columns: cik, form_type, filing_date, accession_number, filing_url
#' @param minimum_lag_days Integer, minimum lag parameter (default: 30)
#' @param allow_amendments Logical, whether to allow 10-K/A (default: FALSE)
#' @param enable_fallback Logical, use fallback mechanism (default: TRUE)
#'
#' @return Data frame with original deal data plus filing selection results
#'
batch_select_filings <- function(deals,
                                all_filings,
                                minimum_lag_days = 30,
                                allow_amendments = FALSE,
                                enable_fallback = TRUE) {
  
  # Validate inputs
  if (!"target_cik" %in% names(deals)) {
    stop("deals must have 'target_cik' column")
  }
  
  if (!"announce_date" %in% names(deals)) {
    stop("deals must have 'announce_date' column")
  }
  
  # Process each deal
  results <- vector("list", nrow(deals))
  
  for (i in seq_len(nrow(deals))) {
    deal <- deals[i, ]
    cik <- deal$target_cik
    announce_date <- deal$announce_date
    
    # Get filing history for this CIK
    filing_hist <- all_filings %>%
      filter(cik == !!cik)
    
    # Select qualifying filing
    selection_result <- select_qualifying_filing(
      filing_history = filing_hist,
      announcement_date = announce_date,
      minimum_lag_days = minimum_lag_days,
      allow_amendments = allow_amendments,
      enable_fallback = enable_fallback
    )
    
    # Combine deal data with selection results
    result_row <- deal
    
    result_row$filing_selection_status <- selection_result$status
    result_row$filing_selection_method <- selection_result$selection_method
    result_row$n_10k_filings <- selection_result$n_10k_filings
    result_row$days_before_announcement <- selection_result$days_before_announcement
    result_row$filing_cutoff_date <- selection_result$cutoff_date
    result_row$most_recent_10k_date <- selection_result$most_recent_10k_date
    result_row$used_fallback <- selection_result$used_fallback
    
    if (!is.null(selection_result$filing)) {
      filing <- selection_result$filing
      result_row$filing_date <- filing$filing_date
      result_row$filing_accession_number <- filing$accession_number
      result_row$filing_url <- filing$filing_url
      result_row$filing_form_type <- filing$form_type
    } else {
      result_row$filing_date <- as.Date(NA)
      result_row$filing_accession_number <- NA_character_
      result_row$filing_url <- NA_character_
      result_row$filing_form_type <- NA_character_
    }
    
    results[[i]] <- result_row
  }
  
  # Combine all results
  dplyr::bind_rows(results)
}

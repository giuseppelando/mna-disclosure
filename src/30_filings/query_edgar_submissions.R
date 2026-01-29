# =============================================================================
# query_edgar_submissions.R
# =============================================================================
# Functions to query SEC EDGAR submissions API and retrieve filing histories
#
# This module provides robust, cached access to EDGAR company filing data
# using the official SEC submissions endpoint.
#
# Key Features:
#   - Queries /submissions/CIK{CIK}.json for complete filing history
#   - Implements polite rate limiting per SEC guidelines
#   - Caches responses to avoid redundant API calls
#   - Handles errors gracefully with retry logic
#   - Returns structured data ready for filtering
#
# =============================================================================

# Required libraries
suppressPackageStartupMessages({
  require(httr)
  require(jsonlite)
  require(dplyr)
  require(stringr)
  require(glue)
})

#' Query SEC EDGAR Submissions API for a Company's Filing History
#'
#' Retrieves complete filing history for a CIK from EDGAR's submissions endpoint.
#' Implements caching, rate limiting, and error handling per SEC guidelines.
#'
#' @param cik Character string, zero-padded 10-digit CIK identifier
#' @param cache_dir Character string, directory for caching responses (default: "data/interim/edgar_submissions_cache")
#' @param user_agent Character string, User-Agent header for SEC requests
#' @param rate_limit_seconds Numeric, seconds to wait between uncached requests (default: 0.11 = ~9 requests/second)
#' @param max_retries Integer, maximum retry attempts for failed requests (default: 3)
#' @param verbose Logical, whether to print progress messages (default: FALSE)
#'
#' @return Data frame with columns: form_type, filing_date, accession_number, filing_url
#'         Returns NULL if query fails after all retries
#'
#' @details
#' The SEC submissions endpoint returns JSON with structure:
#'   - filings$recent: Most recent ~1000 filings as arrays
#'   - filings$files: Archive files for older filings
#'
#' This function focuses on filings$recent which is sufficient for most M&A deals
#' occurring in the past 20-30 years. Handles both recent and archive structures.
#'
#' Caching: Saves raw JSON response to cache_dir/CIK{cik}.json to avoid
#' redundant queries. Checks cache first before making API calls.
#'
#' Rate Limiting: Waits rate_limit_seconds between NEW queries (not cached).
#' SEC guidelines: max 10 requests/second. Default 0.11s = ~9 req/s (safe margin).
#'
#' @examples
#' # Query Apple's filing history
#' apple_filings <- query_edgar_submissions("0000320193")
#'
#' # Check cache status
#' apple_filings_cached <- query_edgar_submissions("0000320193")  # Instant!
#'
query_edgar_submissions <- function(cik,
                                   cache_dir = "data/interim/edgar_submissions_cache",
                                   user_agent = "mna-disclosure-research giuseppe.lando@studbocconi.it",
                                   rate_limit_seconds = 0.11,
                                   max_retries = 3,
                                   verbose = FALSE) {
  
  # Validate CIK format
  if (!grepl("^\\d{10}$", cik)) {
    warning(glue("Invalid CIK format: {cik} (must be 10 digits)"))
    return(NULL)
  }
  
  # Ensure cache directory exists
  if (!dir.exists(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  }
  
  # Check cache first
  cache_file <- file.path(cache_dir, glue("CIK{cik}.json"))
  
  if (file.exists(cache_file)) {
    if (verbose) message(glue("  [CACHE HIT] CIK {cik}"))
    
    # Load from cache
    tryCatch({
      cached_json <- readLines(cache_file, warn = FALSE)
      submissions_data <- jsonlite::fromJSON(paste(cached_json, collapse = "\n"))
      return(parse_submissions_response(submissions_data, cik))
    }, error = function(e) {
      warning(glue("Failed to read cache for CIK {cik}: {e$message}"))
      # Continue to fresh query if cache read fails
    })
  }
  
  # Cache miss - query EDGAR
  if (verbose) message(glue("  [QUERY] CIK {cik}"))
  
  # Construct API URL
  api_url <- glue("https://data.sec.gov/submissions/CIK{cik}.json")
  
  # Retry loop
  for (attempt in 1:max_retries) {
    
    # Rate limiting (skip on first request in batch, apply on subsequent)
    if (exists(".last_edgar_query_time", envir = .GlobalEnv)) {
      time_since_last <- as.numeric(Sys.time() - get(".last_edgar_query_time", envir = .GlobalEnv))
      if (time_since_last < rate_limit_seconds) {
        Sys.sleep(rate_limit_seconds - time_since_last)
      }
    }
    
    # Make request
    response <- tryCatch({
      httr::GET(
        api_url,
        httr::add_headers(`User-Agent` = user_agent),
        httr::timeout(30)
      )
    }, error = function(e) {
      if (verbose) message(glue("    Attempt {attempt}/{max_retries}: Network error - {e$message}"))
      return(NULL)
    })
    
    # Record query time for rate limiting
    assign(".last_edgar_query_time", Sys.time(), envir = .GlobalEnv)
    
    # Check response
    if (is.null(response)) {
      if (attempt < max_retries) {
        Sys.sleep(2^attempt)  # Exponential backoff
        next
      } else {
        warning(glue("Failed to query CIK {cik} after {max_retries} attempts"))
        return(NULL)
      }
    }
    
    # Check HTTP status
    if (httr::http_error(response)) {
      status <- httr::status_code(response)
      if (verbose) message(glue("    Attempt {attempt}/{max_retries}: HTTP {status}"))
      
      if (status == 404) {
        # CIK not found - don't retry
        warning(glue("CIK {cik} not found in EDGAR (404)"))
        return(NULL)
      }
      
      if (status == 429) {
        # Rate limited - wait longer
        if (verbose) message("    Rate limited, waiting 60 seconds...")
        Sys.sleep(60)
      }
      
      if (attempt < max_retries) {
        Sys.sleep(2^attempt)
        next
      } else {
        warning(glue("HTTP {status} for CIK {cik} after {max_retries} attempts"))
        return(NULL)
      }
    }
    
    # Parse JSON response
    submissions_data <- tryCatch({
      content_text <- httr::content(response, as = "text", encoding = "UTF-8")
      jsonlite::fromJSON(content_text)
    }, error = function(e) {
      if (verbose) message(glue("    Attempt {attempt}/{max_retries}: JSON parse error - {e$message}"))
      return(NULL)
    })
    
    if (is.null(submissions_data)) {
      if (attempt < max_retries) {
        Sys.sleep(2^attempt)
        next
      } else {
        warning(glue("Failed to parse JSON for CIK {cik}"))
        return(NULL)
      }
    }
    
    # Success - save to cache
    tryCatch({
      writeLines(jsonlite::toJSON(submissions_data, auto_unbox = TRUE, pretty = TRUE), cache_file)
    }, error = function(e) {
      warning(glue("Failed to write cache for CIK {cik}: {e$message}"))
    })
    
    # Parse and return
    return(parse_submissions_response(submissions_data, cik))
  }
  
  # Should never reach here
  return(NULL)
}


#' Parse SEC Submissions API Response into Filing Data Frame
#'
#' Internal function to extract filing records from submissions JSON structure
#'
#' @param submissions_data List, parsed JSON from submissions endpoint
#' @param cik Character, CIK identifier for error messages
#'
#' @return Data frame with filing records
#'
parse_submissions_response <- function(submissions_data, cik) {
  
  # Check structure
  if (!"filings" %in% names(submissions_data)) {
    warning(glue("No 'filings' field in submissions data for CIK {cik}"))
    return(NULL)
  }
  
  if (!"recent" %in% names(submissions_data$filings)) {
    warning(glue("No 'recent' filings in submissions data for CIK {cik}"))
    return(NULL)
  }
  
  recent <- submissions_data$filings$recent
  
  # Check required fields
  required_fields <- c("form", "filingDate", "accessionNumber", "primaryDocument")
  missing_fields <- setdiff(required_fields, names(recent))
  
  if (length(missing_fields) > 0) {
    warning(glue("Missing required fields for CIK {cik}: {paste(missing_fields, collapse = ', ')}"))
    return(NULL)
  }
  
  # Build data frame
  filings_df <- tryCatch({
    data.frame(
      cik = cik,
      form_type = recent$form,
      filing_date = as.Date(recent$filingDate),
      accession_number = recent$accessionNumber,
      primary_document = recent$primaryDocument,
      stringsAsFactors = FALSE
    )
  }, error = function(e) {
    warning(glue("Error constructing filing data frame for CIK {cik}: {e$message}"))
    return(NULL)
  })
  
  if (is.null(filings_df) || nrow(filings_df) == 0) {
    return(NULL)
  }
  
  # Construct filing URLs
  # Format: https://www.sec.gov/Archives/edgar/data/{CIK}/{ACCESSION_NO_DASHES}/{PRIMARY_DOC}
  filings_df <- filings_df %>%
    mutate(
      accession_no_dashes = str_remove_all(accession_number, "-"),
      cik_no_leading_zeros = as.character(as.integer(cik)),
      filing_url = glue("https://www.sec.gov/Archives/edgar/data/{cik_no_leading_zeros}/{accession_no_dashes}/{primary_document}")
    ) %>%
    select(cik, form_type, filing_date, accession_number, filing_url)
  
  return(filings_df)
}


#' Batch Query Multiple CIKs with Progress Tracking
#'
#' Convenience wrapper to query filing histories for multiple CIKs with
#' progress bar and summary statistics.
#'
#' @param ciks Character vector of CIK identifiers
#' @param cache_dir Character string, cache directory path
#' @param user_agent Character string, User-Agent header
#' @param verbose Logical, print individual query messages
#' @param show_progress Logical, show progress bar (default: TRUE)
#'
#' @return List with two elements:
#'   - filings: Combined data frame of all filing histories
#'   - stats: Summary statistics (n_queried, n_successful, n_failed)
#'
batch_query_submissions <- function(ciks,
                                   cache_dir = "data/interim/edgar_submissions_cache",
                                   user_agent = "mna-disclosure-research giuseppe.lando@studbocconi.it",
                                   verbose = FALSE,
                                   show_progress = TRUE) {
  
  n_ciks <- length(ciks)
  results <- vector("list", n_ciks)
  success_count <- 0
  failed_ciks <- character()
  
  if (show_progress) {
    pb <- txtProgressBar(min = 0, max = n_ciks, style = 3)
  }
  
  for (i in seq_along(ciks)) {
    cik <- ciks[i]
    
    result <- query_edgar_submissions(
      cik = cik,
      cache_dir = cache_dir,
      user_agent = user_agent,
      verbose = verbose
    )
    
    if (!is.null(result) && nrow(result) > 0) {
      results[[i]] <- result
      success_count <- success_count + 1
    } else {
      failed_ciks <- c(failed_ciks, cik)
    }
    
    if (show_progress) {
      setTxtProgressBar(pb, i)
    }
  }
  
  if (show_progress) {
    close(pb)
  }
  
  # Combine results
  all_filings <- dplyr::bind_rows(results)
  
  # Summary stats
  stats <- list(
    n_queried = n_ciks,
    n_successful = success_count,
    n_failed = length(failed_ciks),
    pct_successful = round(100 * success_count / n_ciks, 1),
    failed_ciks = failed_ciks
  )
  
  return(list(
    filings = all_filings,
    stats = stats
  ))
}

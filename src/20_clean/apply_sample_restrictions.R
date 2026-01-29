# =============================================================================
# src/20_clean/apply_sample_restrictions.R
# =============================================================================
# Apply research design sample restrictions in documented cascade
#
# Purpose:
#   Transform the full ingested dataset into the analytical sample by applying
#   inclusion criteria from the research design in explicit sequence. Each
#   exclusion step is logged and documented for thesis methodology chapter.
#
# Design Alignment:
#   Implements DL-02 (unit of analysis), DL-03 (terminal outcomes), DL-04
#   (temporal anchor), DL-09 (control transfer), and optional geography filters.
#
# Input:
#   data/interim/deals_ingested.rds (from read_deals_v2_refined.R)
#
# Output:
#   data/interim/deals_sample_restricted.rds (analytical sample)
#   data/interim/sample_restriction_log.txt (detailed exclusion cascade)
#   data/interim/sample_restriction_summary.json (statistics)
#   data/interim/sample_restriction_flowchart.txt (visual cascade)
#
# Configuration:
#   Edit the CONFIGURATION section below to adjust which filters to apply
#   and their order. This allows easy robustness checks.
#
# Usage:
#   Rscript src/20_clean/apply_sample_restrictions.R
#
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(glue)
  library(fs)
  library(jsonlite)
})

# =============================================================================
# CONFIGURATION
# =============================================================================

# Input file from ingestion
INPUT_FILE <- "data/interim/deals_ingested.rds"

# Output files
OUTPUT_FILE <- "data/interim/deals_sample_restricted.rds"
LOG_FILE <- "data/interim/sample_restriction_log.txt"
SUMMARY_FILE <- "data/interim/sample_restriction_summary.json"
FLOWCHART_FILE <- "data/interim/sample_restriction_flowchart.txt"

# Sample restriction rules (in application order)
# Set enabled = FALSE to skip a filter for robustness checks
RESTRICTIONS <- list(
  
  # DL-02: Unit of analysis requires identifiable target
  target_identifiable = list(
    enabled = TRUE,
    description = "Target has ticker symbol or company name",
    flag = "flag_has_target_id",
    design_reference = "DL-02",
    rationale = "Cannot analyze deals without target identification"
  ),
  
  # DL-04: Temporal anchor - must have announcement date
  announcement_date = list(
    enabled = TRUE,
    description = "Non-missing announcement date",
    flag = "flag_has_announce_date",
    design_reference = "DL-04",
    rationale = "Announcement date is temporal anchor for all timing logic"
  ),
  
  # DL-03: Terminal outcome status for completion analysis
  terminal_outcome = list(
    enabled = TRUE,
    description = "Deal status is Completed or Withdrawn",
    flag = "flag_terminal_outcome",
    design_reference = "DL-03",
    rationale = "Only terminal outcomes suitable for completion probability analysis"
  ),
  
  # DL-09: Control transfer criterion
  control_transfer = list(
    enabled = TRUE,
    description = "Transaction constitutes control transfer (stake >= 50% or acquisition/merger type)",
    flag = "flag_control_transfer",
    design_reference = "DL-09",
    rationale = "Research design focuses on market for corporate control, not minority investments"
  ),
  
  # Optional: US listing requirement
  us_listed = list(
    enabled = FALSE,  # Set to TRUE if final SDC export is not pre-filtered to US targets
    description = "Target listed on US exchange (NYSE, NASDAQ, AMEX)",
    flag = "flag_us_listed",
    design_reference = "DL-01 (implied)",
    rationale = "US market for corporate control; ensures Form 10-K availability"
  ),
  
  # Time period restriction (if needed)
  time_period = list(
    enabled = FALSE,  # Set to TRUE and configure dates below if period restriction needed
    description = "Announcement date within specified period",
    custom_filter = TRUE,
    min_date = as.Date("2002-01-01"),  # Adjust as needed
    max_date = as.Date("2022-12-31"),  # Adjust as needed
    design_reference = "DL-18",
    rationale = "Period selection for comparability and 10-K section consistency"
  )
)

# Additional data quality filters (optional, can be enabled for robustness)
QUALITY_FILTERS <- list(
  
  # Remove deals with invalid dates
  date_logic = list(
    enabled = TRUE,
    description = "Completion/withdrawal date not before announcement date",
    custom_filter = TRUE,
    design_reference = "Data quality",
    rationale = "Logical date ordering requirement"
  ),
  
  # Remove deals with negative values
  negative_values = list(
    enabled = TRUE,
    description = "Non-negative deal value and offer price (where non-missing)",
    custom_filter = TRUE,
    design_reference = "Data quality",
    rationale = "Negative values indicate data errors"
  )
)

# =============================================================================
# SETUP
# =============================================================================

# Ensure output directory exists
fs::dir_create(dirname(OUTPUT_FILE))

# Set up logging
log_conn <- file(LOG_FILE, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  message(full_msg)
}

# Tracking structure for cascade
cascade_steps <- list()

log_msg("=================================================================", "INFO")
log_msg("SAMPLE RESTRICTION CASCADE", "INFO")
log_msg("=================================================================", "INFO")
log_msg(glue("Input: {INPUT_FILE}"), "INFO")
log_msg(glue("Output: {OUTPUT_FILE}"), "INFO")
log_msg(glue("Start time: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"), "INFO")

# =============================================================================
# LOAD DATA
# =============================================================================

log_msg("", "INFO")
log_msg("Loading ingested dataset...", "INFO")

if (!file.exists(INPUT_FILE)) {
  log_msg(glue("Input file not found: {INPUT_FILE}"), "ERROR")
  stop("Input file missing. Run read_deals_v2_refined.R first.")
}

df <- readRDS(INPUT_FILE)

n_start <- nrow(df)
log_msg(glue("Loaded: {n_start} deals"), "INFO")

# Verify required flag columns exist
required_flags <- c(
  "flag_has_target_id", "flag_has_announce_date", "flag_terminal_outcome",
  "flag_control_transfer", "flag_has_offer_price"
)

missing_flags <- setdiff(required_flags, names(df))
if (length(missing_flags) > 0) {
  log_msg(glue("Missing required flag columns: {paste(missing_flags, collapse = ', ')}"), "ERROR")
  stop("Input file missing required sample filter flags")
}

# Record starting point
cascade_steps[["start"]] <- list(
  step_name = "Starting sample (after ingestion)",
  n_before = n_start,
  n_excluded = 0,
  n_after = n_start,
  pct_remaining = 100,
  pct_of_start = 100
)

# =============================================================================
# APPLY RESTRICTIONS IN CASCADE
# =============================================================================

log_msg("", "INFO")
log_msg("Applying sample restrictions in cascade...", "INFO")
log_msg("-----------------------------------------------------------------", "INFO")

# Helper function to apply a flag-based filter
apply_filter <- function(df, filter_spec, step_num) {
  n_before <- nrow(df)
  
  log_msg("", "INFO")
  log_msg(glue("STEP {step_num}: {filter_spec$description}"), "INFO")
  log_msg(glue("  Design reference: {filter_spec$design_reference}"), "DEBUG")
  log_msg(glue("  Rationale: {filter_spec$rationale}"), "DEBUG")
  
  # Check if this is a custom filter (default to FALSE if not specified)
  is_custom <- isTRUE(filter_spec$custom_filter)
  
  # Apply filter based on flag
  if (!is_custom) {
    flag_col <- filter_spec$flag
    
    if (!flag_col %in% names(df)) {
      log_msg(glue("  ✗ Flag column '{flag_col}' not found - skipping"), "ERROR")
      return(df)
    }
    
    # Count how many pass the filter
    n_pass <- sum(df[[flag_col]], na.rm = TRUE)
    n_fail <- n_before - n_pass
    
    log_msg(glue("  Before filter: {n_before} deals"), "INFO")
    log_msg(glue("  Passing filter: {n_pass} deals"), "INFO")
    log_msg(glue("  Failing filter: {n_fail} deals ({round(100*n_fail/n_before, 1)}%)"), "INFO")
    
    # Apply filter (keep only rows where flag is TRUE)
    df_filtered <- df %>%
      filter(!!sym(flag_col) == TRUE)
    
  } else {
    # Custom filter logic (not flag-based)
    df_filtered <- df
    n_fail <- 0  # Will be computed by custom logic
    
    # Placeholder for custom filters to be implemented inline below
    log_msg("  Custom filter - see implementation", "INFO")
  }
  
  n_after <- nrow(df_filtered)
  n_excluded <- n_before - n_after
  pct_remaining <- round(100 * n_after / n_before, 1)
  pct_of_start <- round(100 * n_after / n_start, 1)
  
  log_msg(glue("  After filter: {n_after} deals"), "INFO")
  log_msg(glue("  Excluded: {n_excluded} deals"), "INFO")
  log_msg(glue("  Remaining: {pct_remaining}% of previous step, {pct_of_start}% of start"), "INFO")
  
  # Record this step
  cascade_steps[[paste0("step_", step_num)]] <<- list(
    step_name = filter_spec$description,
    design_reference = filter_spec$design_reference,
    n_before = n_before,
    n_excluded = n_excluded,
    n_after = n_after,
    pct_remaining = pct_remaining,
    pct_of_start = pct_of_start
  )
  
  return(df_filtered)
}

# Apply each enabled restriction in order
step_num <- 1

for (restriction_name in names(RESTRICTIONS)) {
  restriction <- RESTRICTIONS[[restriction_name]]
  
  if (restriction$enabled) {
    
    # Handle time period filter specially (custom filter)
    if (restriction_name == "time_period" && isTRUE(restriction$custom_filter)) {
      n_before <- nrow(df)
      
      log_msg("", "INFO")
      log_msg(glue("STEP {step_num}: {restriction$description}"), "INFO")
      log_msg(glue("  Period: {restriction$min_date} to {restriction$max_date}"), "INFO")
      log_msg(glue("  Design reference: {restriction$design_reference}"), "DEBUG")
      
      df <- df %>%
        filter(
          announce_date >= restriction$min_date,
          announce_date <= restriction$max_date
        )
      
      n_after <- nrow(df)
      n_excluded <- n_before - n_after
      pct_remaining <- round(100 * n_after / n_before, 1)
      pct_of_start <- round(100 * n_after / n_start, 1)
      
      log_msg(glue("  Excluded: {n_excluded} deals outside period"), "INFO")
      log_msg(glue("  Remaining: {n_after} deals ({pct_of_start}% of start)"), "INFO")
      
      cascade_steps[[paste0("step_", step_num)]] <- list(
        step_name = restriction$description,
        design_reference = restriction$design_reference,
        n_before = n_before,
        n_excluded = n_excluded,
        n_after = n_after,
        pct_remaining = pct_remaining,
        pct_of_start = pct_of_start
      )
      
      step_num <- step_num + 1
      
    } else if (!isTRUE(restriction$custom_filter)) {
      # Standard flag-based filter
      df <- apply_filter(df, restriction, step_num)
      step_num <- step_num + 1
    }
  }
}

# Apply quality filters if enabled
for (quality_name in names(QUALITY_FILTERS)) {
  quality_filter <- QUALITY_FILTERS[[quality_name]]
  
  if (quality_filter$enabled) {
    n_before <- nrow(df)
    
    log_msg("", "INFO")
    log_msg(glue("QUALITY CHECK {step_num}: {quality_filter$description}"), "INFO")
    
    if (quality_name == "date_logic") {
      # Remove deals where completion/withdrawal date precedes announcement
      df <- df %>%
        filter(
          is.na(complete_date) | complete_date >= announce_date,
          is.na(withdrawn_date) | withdrawn_date >= announce_date
        )
      
    } else if (quality_name == "negative_values") {
      # Remove deals with negative prices/values
      df <- df %>%
        filter(
          is.na(deal_value_eur_th) | deal_value_eur_th >= 0,
          is.na(offer_price_eur) | offer_price_eur >= 0
        )
    }
    
    n_after <- nrow(df)
    n_excluded <- n_before - n_after
    pct_of_start <- round(100 * n_after / n_start, 1)
    
    log_msg(glue("  Excluded: {n_excluded} deals"), "INFO")
    log_msg(glue("  Remaining: {n_after} deals ({pct_of_start}% of start)"), "INFO")
    
    cascade_steps[[paste0("quality_", quality_name)]] <- list(
      step_name = quality_filter$description,
      design_reference = quality_filter$design_reference,
      n_before = n_before,
      n_excluded = n_excluded,
      n_after = n_after,
      pct_remaining = round(100 * n_after / n_before, 1),
      pct_of_start = pct_of_start
    )
    
    step_num <- step_num + 1
  }
}

# =============================================================================
# FINAL SAMPLE SUMMARY
# =============================================================================

n_final <- nrow(df)
n_total_excluded <- n_start - n_final
pct_retained <- round(100 * n_final / n_start, 1)

log_msg("", "INFO")
log_msg("=================================================================", "INFO")
log_msg("SAMPLE RESTRICTION COMPLETE", "INFO")
log_msg("=================================================================", "INFO")
log_msg(glue("Starting sample: {n_start} deals"), "INFO")
log_msg(glue("Final sample: {n_final} deals"), "INFO")
log_msg(glue("Total excluded: {n_total_excluded} deals ({round(100*n_total_excluded/n_start, 1)}%)"), "INFO")
log_msg(glue("Retention rate: {pct_retained}%"), "INFO")

# =============================================================================
# SAVE OUTPUTS
# =============================================================================

log_msg("", "INFO")
log_msg("Saving outputs...", "INFO")

# Save restricted dataset
saveRDS(df, OUTPUT_FILE)
log_msg(glue("✓ Saved: {OUTPUT_FILE}"), "INFO")

# Save cascade summary as JSON
summary_data <- list(
  input_file = INPUT_FILE,
  output_file = OUTPUT_FILE,
  processing_date = as.character(Sys.time()),
  n_start = n_start,
  n_final = n_final,
  n_excluded = n_total_excluded,
  pct_retained = pct_retained,
  restrictions_applied = names(RESTRICTIONS)[sapply(RESTRICTIONS, function(x) x$enabled)],
  cascade_steps = cascade_steps
)

jsonlite::write_json(
  summary_data,
  SUMMARY_FILE,
  pretty = TRUE,
  auto_unbox = TRUE
)
log_msg(glue("✓ Saved: {SUMMARY_FILE}"), "INFO")

# Create visual flowchart
flowchart_lines <- c(
  "=======================================================================",
  "SAMPLE RESTRICTION CASCADE FLOWCHART",
  "=======================================================================",
  "",
  glue("Starting sample: {n_start} deals"),
  ""
)

for (step_name in names(cascade_steps)) {
  step <- cascade_steps[[step_name]]
  if (step_name != "start") {
    flowchart_lines <- c(
      flowchart_lines,
      "    |",
      "    v",
      glue("[ {step$step_name} ]"),
      glue("    Excluded: {step$n_excluded} deals"),
      glue("    Remaining: {step$n_after} deals ({step$pct_of_start}% of start)"),
      ""
    )
  }
}

flowchart_lines <- c(
  flowchart_lines,
  "    |",
  "    v",
  glue("Final analytical sample: {n_final} deals ({pct_retained}% retention)"),
  "",
  "======================================================================="
)

writeLines(flowchart_lines, FLOWCHART_FILE)
log_msg(glue("✓ Saved: {FLOWCHART_FILE}"), "INFO")

# =============================================================================
# COMPLETION
# =============================================================================

log_msg("", "INFO")
log_msg("Sample restriction pipeline complete", "INFO")
log_msg(glue("End time: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}"), "INFO")

close(log_conn)

# Print summary to console
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("SAMPLE RESTRICTION COMPLETE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat(glue("Input:  {INPUT_FILE}"), "\n")
cat(glue("Output: {OUTPUT_FILE}"), "\n")
cat(glue("Sample: {n_start} → {n_final} deals ({pct_retained}% retained)"), "\n")
cat("\n")
cat("Exclusion cascade:\n")
for (step_name in names(cascade_steps)[-1]) {  # Skip "start"
  step <- cascade_steps[[step_name]]
  cat(glue("  {step$step_name}: -{step$n_excluded} deals"), "\n")
}
cat("\n")
cat(glue("See {FLOWCHART_FILE} for visual cascade"), "\n")
cat(glue("See {LOG_FILE} for detailed log"), "\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

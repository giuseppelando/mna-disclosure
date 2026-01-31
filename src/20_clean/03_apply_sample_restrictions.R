# =============================================================================
# src/20_clean/apply_sample_restrictions_v2.R
# =============================================================================
# COMPREHENSIVE SAMPLE RESTRICTIONS FOR M&A RESEARCH
#
# Aligned with research design principles:
# - Conditional on announcement (no entry margin)
# - Terminal outcomes only (completed/withdrawn)
# - Control transfer focus (acquisition context)
# - US public targets (standardized disclosure)
# - Clean premium data where available
#
# Input:  data/interim/deals_with_premium.rds (or deals_ingested.rds)
# Output: data/processed/deals_restricted.rds
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(glue)
})

# =============================================================================
# SETUP LOGGING
# =============================================================================

log_file <- "data/interim/sample_restrictions_log.txt"
dir.create("data/interim", recursive = TRUE, showWarnings = FALSE)
dir.create("data/processed", recursive = TRUE, showWarnings = FALSE)

log_conn <- file(log_file, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  message(full_msg)
}

log_msg(paste(rep("=", 70), collapse = ""))
log_msg("SAMPLE RESTRICTION CASCADE - COMPREHENSIVE VERSION")
log_msg(paste(rep("=", 70), collapse = ""))

# =============================================================================
# STEP 1: LOAD DATA
# =============================================================================

log_msg("")
log_msg("STEP 1: Loading data...")
log_msg(paste(rep("-", 70), collapse = ""))

# Try to load premium version first, fall back to ingested
if (file.exists("data/interim/deals_with_premium.rds")) {
  df <- readRDS("data/interim/deals_with_premium.rds")
  log_msg("Loaded: deals_with_premium.rds")
} else if (file.exists("data/interim/deals_ingested.rds")) {
  df <- readRDS("data/interim/deals_ingested.rds")
  log_msg("Loaded: deals_ingested.rds")
} else {
  stop("No input file found. Run ingestion first.")
}

n_start <- nrow(df)
log_msg(glue("Starting sample: {n_start} deals"))

# Show available columns for debugging
log_msg(glue("Available columns: {ncol(df)}"))
log_msg("Key columns present:", "DEBUG")
key_cols <- c("deal_id", "target_name", "deal_status", "deal_completed", 
              "target_nation", "percentage_of_cash", "percentage_of_stock",
              "deal_premium")
for (col in key_cols) {
  present <- col %in% names(df)
  log_msg(glue("  {col}: {if(present) '✓' else '✗'}"), "DEBUG")
}

# =============================================================================
# STEP 2: IDENTIFY STAKE COLUMNS DYNAMICALLY
# =============================================================================

log_msg("")
log_msg("STEP 2: Identifying stake columns...")
log_msg(paste(rep("-", 70), collapse = ""))

# Find stake columns (various naming conventions)
initial_stake_col <- names(df)[str_detect(names(df), "percent.*owned.*before|initial.*stake|percent.*before")][1]
final_stake_col <- names(df)[str_detect(names(df), "percent.*acquired|final.*stake|percent.*after|percent.*of.*shares.*acquired")][1]

if (!is.na(initial_stake_col)) {
  log_msg(glue("Found initial stake: {initial_stake_col}"))
} else {
  log_msg("No initial stake column found", "WARN")
}

if (!is.na(final_stake_col)) {
  log_msg(glue("Found final stake: {final_stake_col}"))
  
  # Rename for consistency
  df <- df %>%
    rename(final_stake_pct = !!sym(final_stake_col))
  
  log_msg(glue("Summary of final_stake_pct:"))
  log_msg(glue("  Min: {round(min(df$final_stake_pct, na.rm=TRUE), 1)}%"), "DEBUG")
  log_msg(glue("  Median: {round(median(df$final_stake_pct, na.rm=TRUE), 1)}%"), "DEBUG")
  log_msg(glue("  Max: {round(max(df$final_stake_pct, na.rm=TRUE), 1)}%"), "DEBUG")
  log_msg(glue("  Missing: {sum(is.na(df$final_stake_pct))} ({round(100*sum(is.na(df$final_stake_pct))/n_start,1)}%)"), "DEBUG")
} else {
  log_msg("WARNING: No final stake column found - cannot apply control transfer filter", "WARN")
}

# =============================================================================
# STEP 3: RESTRICTION CASCADE (RESEARCH DESIGN ALIGNED)
# =============================================================================

log_msg("")
log_msg("STEP 3: Applying restriction cascade...")
log_msg(paste(rep("-", 70), collapse = ""))

# Track sample at each step
cascade <- data.frame(
  step = character(),
  description = character(),
  n_removed = integer(),
  n_remaining = integer(),
  pct_remaining = numeric(),
  stringsAsFactors = FALSE
)

add_cascade_step <- function(step_name, step_desc, n_before, n_after) {
  cascade <<- rbind(cascade, data.frame(
    step = step_name,
    description = step_desc,
    n_removed = n_before - n_after,
    n_remaining = n_after,
    pct_remaining = round(100 * n_after / n_start, 1),
    stringsAsFactors = FALSE
  ))
}

# Initial state
add_cascade_step("0", "Initial sample", n_start, n_start)
df_working <- df

# --------------------------------------------------------------------------
# RESTRICTION 1: Terminal outcomes only (completed or withdrawn)
# --------------------------------------------------------------------------
log_msg("")
log_msg("Restriction 1: Terminal outcomes only...")

n_before <- nrow(df_working)

if ("deal_completed" %in% names(df_working)) {
  df_working <- df_working %>%
    filter(!is.na(deal_completed))
  
  log_msg(glue("  Using deal_completed indicator"))
} else if ("deal_status" %in% names(df_working)) {
  df_working <- df_working %>%
    filter(
      str_to_lower(str_trim(deal_status)) %in% c("completed", "withdrawn")
    )
  
  log_msg(glue("  Using deal_status classification"))
} else {
  log_msg("  WARNING: Cannot identify terminal outcomes - skipping", "WARN")
}

n_after <- nrow(df_working)
add_cascade_step("1", "Non-terminal outcomes", n_before, n_after)
log_msg(glue("  Removed: {n_before - n_after} ({round(100*(n_before-n_after)/n_before,1)}%)"))
log_msg(glue("  Remaining: {n_after}"))

# --------------------------------------------------------------------------
# RESTRICTION 2: Control transfer (stake ≥ 50%)
# --------------------------------------------------------------------------
log_msg("")
log_msg("Restriction 2: Control transfer (stake ≥ 50%)...")

n_before <- nrow(df_working)

if ("final_stake_pct" %in% names(df_working)) {
  # First, remove deals with missing stake
  n_missing_stake <- sum(is.na(df_working$final_stake_pct))
  
  df_working <- df_working %>%
    filter(
      !is.na(final_stake_pct),  # Must have stake data
      final_stake_pct >= 50      # Must be control transfer
    )
  
  log_msg(glue("  Removed missing stake: {n_missing_stake}"))
  log_msg(glue("  Removed non-control (<50%): {n_before - nrow(df_working) - n_missing_stake}"))
} else {
  log_msg("  WARNING: Cannot apply stake filter - skipping", "WARN")
}

n_after <- nrow(df_working)
add_cascade_step("2", "Non-control transactions + missing stake", n_before, n_after)
log_msg(glue("  Removed: {n_before - n_after} ({round(100*(n_before-n_after)/n_before,1)}%)"))
log_msg(glue("  Remaining: {n_after}"))

# --------------------------------------------------------------------------
# RESTRICTION 3: US public targets
# --------------------------------------------------------------------------
log_msg("")
log_msg("Restriction 3: US public targets...")

n_before <- nrow(df_working)

if ("target_nation" %in% names(df_working)) {
  df_working <- df_working %>%
    filter(
      str_detect(str_to_upper(str_trim(target_nation)), "^US$|^USA$|UNITED STATES")
    )
  
  log_msg(glue("  Filtered to US targets"))
} else {
  log_msg("  WARNING: No target_nation column - skipping", "WARN")
}

n_after <- nrow(df_working)
add_cascade_step("3", "Non-US targets", n_before, n_after)
log_msg(glue("  Removed: {n_before - n_after} ({round(100*(n_before-n_after)/n_before,1)}%)"))
log_msg(glue("  Remaining: {n_after}"))

# --------------------------------------------------------------------------
# RESTRICTION 4 (OPTIONAL): LBO exclusion
# --------------------------------------------------------------------------
log_msg("")
log_msg("Restriction 4 (OPTIONAL): LBO exclusion...")

n_before <- nrow(df_working)

# Look for LBO indicators in deal type or acquisition technique
lbo_cols <- names(df_working)[str_detect(names(df_working), "deal.*type|acquisition.*technique")]

if (length(lbo_cols) > 0) {
  log_msg(glue("  Checking columns: {paste(lbo_cols, collapse=', ')}"))
  
  # Create LBO flag
  df_working <- df_working %>%
    mutate(
      is_lbo = rowSums(across(
        all_of(lbo_cols),
        ~str_detect(str_to_lower(as.character(.x)), "lbo|leveraged.*buyout|management.*buyout|mbo")
      ), na.rm = TRUE) > 0
    )
  
  n_lbo <- sum(df_working$is_lbo, na.rm = TRUE)
  log_msg(glue("  Found {n_lbo} LBOs ({round(100*n_lbo/n_before,1)}%)"))
  
  # Optionally exclude (commented out by default - uncomment if needed)
  # df_working <- df_working %>% filter(!is_lbo)
  # log_msg("  LBOs excluded")
  
  log_msg("  LBOs flagged but NOT excluded (adjust if needed)", "WARN")
} else {
  log_msg("  No LBO indicators found - skipping")
}

n_after <- nrow(df_working)
add_cascade_step("4", "LBOs (if excluded)", n_before, n_after)
log_msg(glue("  Remaining: {n_after}"))

# --------------------------------------------------------------------------
# RESTRICTION 5: Missing critical identifiers
# --------------------------------------------------------------------------
log_msg("")
log_msg("Restriction 5: Missing critical identifiers...")

n_before <- nrow(df_working)

df_working <- df_working %>%
  filter(
    !is.na(deal_id),
    !is.na(target_name)
  )

# Check for ticker (important for CIK matching)
if ("target_ticker" %in% names(df_working)) {
  n_no_ticker <- sum(is.na(df_working$target_ticker))
  log_msg(glue("  Deals without ticker: {n_no_ticker} ({round(100*n_no_ticker/n_before,1)}%)"), "WARN")
  log_msg("  (Kept in sample - CIK matching will handle)")
}

n_after <- nrow(df_working)
add_cascade_step("5", "Missing deal_id or target_name", n_before, n_after)
log_msg(glue("  Removed: {n_before - n_after}"))
log_msg(glue("  Remaining: {n_after}"))

# =============================================================================
# STEP 4: DATA QUALITY FLAGS
# =============================================================================

log_msg("")
log_msg("STEP 4: Creating data quality flags...")
log_msg(paste(rep("-", 70), collapse = ""))

# Precompute column existence once (stable, avoids mutate scoping issues)
has_col_deal_premium <- "deal_premium" %in% names(df_working)
has_col_target_ticker <- "target_ticker" %in% names(df_working)
has_col_target_cusip  <- "target_cusip" %in% names(df_working)
has_col_payment_clean <- "payment_method_clean" %in% names(df_working)
has_col_cash          <- "percentage_of_cash" %in% names(df_working)
has_col_stock         <- "percentage_of_stock" %in% names(df_working)

# Identify key columns once
price_cols <- names(df_working)[str_detect(names(df_working), "price")]
has_offer_price_col <- any(str_detect(price_cols, "paid|acquir"))
has_target_price_col <- any(str_detect(price_cols, "target.*prior"))

deal_value_cols <- names(df_working)[str_detect(names(df_working), "deal.*value|transaction.*value")]
deal_value_col <- if (length(deal_value_cols) > 0) deal_value_cols[1] else NA_character_

announce_date_cols <- names(df_working)[str_detect(names(df_working), "date.*announced")]
announce_date_col <- if (length(announce_date_cols) > 0) announce_date_cols[1] else NA_character_

df_working <- df_working %>%
  mutate(
    # Premium data availability (vectorised; safe if column absent)
    has_premium = if (has_col_deal_premium) !is.na(deal_premium) else FALSE,
    
    # Price data components (dataset-level existence, repeated per row)
    has_price_data = (has_offer_price_col & has_target_price_col),
    
    # Identifier completeness
    has_ticker = if (has_col_target_ticker) !is.na(target_ticker) else FALSE,
    has_cusip  = if (has_col_target_cusip)  !is.na(target_cusip)  else FALSE,
    
    # Deal characteristics completeness
    has_deal_value = if (!is.na(deal_value_col)) !is.na(.data[[deal_value_col]]) else FALSE,
    
    has_payment_method =
      if (has_col_payment_clean) {
        !is.na(payment_method_clean)
      } else if (has_col_cash && has_col_stock) {
        !is.na(percentage_of_cash) | !is.na(percentage_of_stock)
      } else {
        FALSE
      },
    
    # Dates completeness
    has_announce_date = if (!is.na(announce_date_col)) !is.na(.data[[announce_date_col]]) else FALSE,
    
    # Overall quality score (0-1)
    data_quality_score = (
      as.numeric(has_premium) * 0.3 +
        as.numeric(has_ticker) * 0.2 +
        as.numeric(has_deal_value) * 0.2 +
        as.numeric(has_payment_method) * 0.15 +
        as.numeric(has_announce_date) * 0.15
    )
  )

# Quality summary
quality_summary <- df_working %>%
  summarise(
    n = n(),
    pct_premium = round(100 * sum(has_premium, na.rm=TRUE) / n(), 1),
    pct_ticker = round(100 * sum(has_ticker, na.rm=TRUE) / n(), 1),
    pct_deal_value = round(100 * sum(has_deal_value, na.rm=TRUE) / n(), 1),
    pct_payment = round(100 * sum(has_payment_method, na.rm=TRUE) / n(), 1),
    mean_quality = round(mean(data_quality_score, na.rm=TRUE), 2)
  )

log_msg("Data quality summary:")
log_msg(glue("  Premium data: {quality_summary$pct_premium}%"))
log_msg(glue("  Ticker: {quality_summary$pct_ticker}%"))
log_msg(glue("  Deal value: {quality_summary$pct_deal_value}%"))
log_msg(glue("  Payment method: {quality_summary$pct_payment}%"))
log_msg(glue("  Mean quality score: {quality_summary$mean_quality}"))

# Prepare for CIK matching (add empty column)
df_working <- df_working %>%
  mutate(target_cik = NA_character_)

if (!("target_cik" %in% names(df_working))) {
  df_working <- df_working %>% mutate(target_cik = NA_character_)
}

log_msg("Added target_cik column for CIK matching")

# =============================================================================
# STEP 5: SAVE OUTPUTS
# =============================================================================

log_msg("")
log_msg("STEP 5: Saving outputs...")
log_msg(paste(rep("-", 70), collapse = ""))

# Save restricted sample
output_path <- "data/processed/deals_restricted.rds"
saveRDS(df_working, output_path)
log_msg(glue("Saved: {output_path}"))

# Save cascade report
cascade_path <- "data/interim/sample_cascade.csv"
write.csv(cascade, cascade_path, row.names = FALSE)
log_msg(glue("Saved: {cascade_path}"))

# =============================================================================
# STEP 6: SUMMARY REPORT
# =============================================================================

log_msg("")
log_msg(paste(rep("=", 70), collapse = ""))
log_msg("RESTRICTION CASCADE COMPLETE")
log_msg(paste(rep("=", 70), collapse = ""))

log_msg("")
log_msg("Sample reduction:")
for (i in 2:nrow(cascade)) {
  log_msg(glue("  Step {cascade$step[i]}: -{cascade$n_removed[i]} ({cascade$description[i]})"))
}

log_msg("")
log_msg(glue("Final sample: {nrow(df_working)} deals ({round(100*nrow(df_working)/n_start,1)}% of initial)"))

log_msg("")
log_msg("Data quality indicators:")
log_msg(glue("  Premium available: {sum(df_working$has_premium, na.rm=TRUE)} deals"))
log_msg(glue("  Ticker available: {sum(df_working$has_ticker, na.rm=TRUE)} deals"))
log_msg(glue("  Ready for CIK matching: {sum(df_working$has_ticker, na.rm=TRUE)} deals"))

close(log_conn)

# Console summary
cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║         SAMPLE RESTRICTION CASCADE - COMPLETE                    ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Initial sample:     %-44d ║\n", n_start))
cat(sprintf("║ Final sample:       %-44d ║\n", nrow(df_working)))
cat(sprintf("║ Retention rate:     %.1f%%%38s ║\n", 100*nrow(df_working)/n_start, ""))
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ Restrictions applied:                                            ║\n")
for (i in 2:nrow(cascade)) {
  if (cascade$n_removed[i] > 0) {
    cat(sprintf("║   %d. %-42s -%5d ║\n", 
                as.integer(cascade$step[i]),
                substr(cascade$description[i], 1, 42),
                cascade$n_removed[i]))
  }
}
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ Data quality:                                                    ║\n")
cat(sprintf("║   Premium data:        %5.1f%%%33s ║\n", quality_summary$pct_premium, ""))
cat(sprintf("║   Ticker (for CIK):    %5.1f%%%33s ║\n", quality_summary$pct_ticker, ""))
cat(sprintf("║   Deal value:          %5.1f%%%33s ║\n", quality_summary$pct_deal_value, ""))
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ Output: data/processed/deals_restricted.rds                      ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")
cat("\n")
cat("✓ Next: source('src/15_historical_cik/resolve_historical_tickers_complete.R')\n\n")

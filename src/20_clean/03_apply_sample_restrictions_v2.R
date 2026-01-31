# =============================================================================
# src/20_clean/03_apply_sample_restrictions.R
# =============================================================================
# STRICT SAMPLE RESTRICTIONS FOR M&A RESEARCH
#
# Applied filters (STRICT version):
#   1. Terminal outcomes only (completed/withdrawn)
#   2. Control transfer (stake ≥ 50%)
#   3. US public targets
#   4. Premium calculable (offer price + target price available)
#   5. Payment method present
#   6. Deal value ≥ $10M
#   7. Period 2006-2023 (Item 1A Risk Factors mandatory)
#
# Input:  data/interim/deals_ingested.rds
# Output: data/processed/deals_restricted.rds
#         data/processed/deals_restricted.xlsx (for manual CIK matching)
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(glue)
  library(openxlsx)
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
log_msg("STRICT SAMPLE RESTRICTION CASCADE")
log_msg(paste(rep("=", 70), collapse = ""))

# =============================================================================
# STEP 1: LOAD DATA
# =============================================================================

log_msg("")
log_msg("STEP 1: Loading data...")
log_msg(paste(rep("-", 70), collapse = ""))

if (file.exists("data/interim/deals_ingested.rds")) {
  df <- readRDS("data/interim/deals_ingested.rds")
  log_msg("Loaded: deals_ingested.rds")
} else {
  stop("No input file found. Run ingestion first.")
}

n_start <- nrow(df)
log_msg(glue("Starting sample: {n_start} deals"))
log_msg(glue("Columns: {ncol(df)}"))

# =============================================================================
# STEP 2: IDENTIFY KEY COLUMNS
# =============================================================================

log_msg("")
log_msg("STEP 2: Identifying key columns...")
log_msg(paste(rep("-", 70), collapse = ""))

# Stake columns
final_stake_col <- names(df)[str_detect(names(df), "percent.*acquired|final.*stake|percent.*after|percent.*of.*shares.*acquired")][1]

if (!is.na(final_stake_col)) {
  log_msg(glue("Found final stake: {final_stake_col}"))
  df <- df %>% rename(final_stake_pct = !!sym(final_stake_col))
} else {
  log_msg("WARNING: No final stake column found", "WARN")
}

# Price columns
offer_price_cols <- c(
  "share_price_paid_by_acquiror_for_target_shares_usd",
  "consideration_price_per_share_usd"
)

target_price_cols <- c(
  "target_share_price_1_week_prior_to_announcement_usd",
  "target_share_price_1_day_prior_to_announcement_usd",
  "target_share_price_4_weeks_prior_to_announcement_usd"
)

# Deal value
deal_value_cols <- names(df)[str_detect(names(df), "deal.*value.*usd.*million")]
deal_value_col <- if(length(deal_value_cols) > 0) deal_value_cols[1] else NA

if(!is.na(deal_value_col)) {
  log_msg(glue("Found deal value: {deal_value_col}"))
  df <- df %>% rename(deal_value_usd_millions = !!sym(deal_value_col))
}

# Year announced
if("date_announced" %in% names(df) && !("year_announced" %in% names(df))) {
  df <- df %>%
    mutate(year_announced = as.integer(format(date_announced, "%Y")))
  log_msg("Created year_announced from date_announced")
}

# =============================================================================
# STEP 3: RESTRICTION CASCADE
# =============================================================================

log_msg("")
log_msg("STEP 3: Applying STRICT restriction cascade...")
log_msg(paste(rep("-", 70), collapse = ""))

# Track cascade
cascade <- data.frame(
  step = character(),
  description = character(),
  n_before = integer(),
  n_after = integer(),
  n_removed = integer(),
  pct_retained = numeric(),
  stringsAsFactors = FALSE
)

add_step <- function(step_num, desc, n_b, n_a) {
  cascade <<- rbind(cascade, data.frame(
    step = as.character(step_num),
    description = desc,
    n_before = n_b,
    n_after = n_a,
    n_removed = n_b - n_a,
    pct_retained = round(100 * n_a / n_start, 1),
    stringsAsFactors = FALSE
  ))
}

df_working <- df
add_step(0, "Initial sample", n_start, n_start)

# --------------------------------------------------------------------------
# FILTER 1: Terminal outcomes only
# --------------------------------------------------------------------------
log_msg("")
log_msg("FILTER 1: Terminal outcomes only...")

n_before <- nrow(df_working)

if("deal_completed" %in% names(df_working)) {
  df_working <- df_working %>% filter(!is.na(deal_completed))
} else if("deal_status" %in% names(df_working)) {
  df_working <- df_working %>%
    filter(str_to_lower(str_trim(deal_status)) %in% c("completed", "withdrawn"))
}

n_after <- nrow(df_working)
add_step(1, "Terminal outcomes", n_before, n_after)
log_msg(glue("  Removed: {n_before - n_after}  |  Remaining: {n_after}"))

# --------------------------------------------------------------------------
# FILTER 2: Control transfer (≥50%)
# --------------------------------------------------------------------------
log_msg("")
log_msg("FILTER 2: Control transfer (≥50%)...")

n_before <- nrow(df_working)

if("final_stake_pct" %in% names(df_working)) {
  df_working <- df_working %>%
    filter(!is.na(final_stake_pct), final_stake_pct >= 50)
}

n_after <- nrow(df_working)
add_step(2, "Control transfer ≥50%", n_before, n_after)
log_msg(glue("  Removed: {n_before - n_after}  |  Remaining: {n_after}"))

# --------------------------------------------------------------------------
# FILTER 3: US public targets
# --------------------------------------------------------------------------
log_msg("")
log_msg("FILTER 3: US public targets...")

n_before <- nrow(df_working)

if("target_nation" %in% names(df_working)) {
  df_working <- df_working %>%
    filter(str_detect(str_to_upper(str_trim(target_nation)), "^US$|^USA$|UNITED STATES"))
}

n_after <- nrow(df_working)
add_step(3, "US public targets", n_before, n_after)
log_msg(glue("  Removed: {n_before - n_after}  |  Remaining: {n_after}"))

# --------------------------------------------------------------------------
# FILTER 4: Premium calculable (CRITICAL)
# --------------------------------------------------------------------------
log_msg("")
log_msg("FILTER 4: Premium calculable...")

n_before <- nrow(df_working)

# Check availability of price inputs
offer_price_available <- rowSums(!is.na(df_working[, offer_price_cols[offer_price_cols %in% names(df_working)], drop=FALSE])) > 0
target_price_available <- rowSums(!is.na(df_working[, target_price_cols[target_price_cols %in% names(df_working)], drop=FALSE])) > 0

df_working <- df_working %>%
  filter(offer_price_available & target_price_available)

n_after <- nrow(df_working)
add_step(4, "Premium calculable", n_before, n_after)
log_msg(glue("  Removed: {n_before - n_after}  |  Remaining: {n_after}"))

# --------------------------------------------------------------------------
# FILTER 5: Payment method present
# --------------------------------------------------------------------------
log_msg("")
log_msg("FILTER 5: Payment method present...")

n_before <- nrow(df_working)

if("payment_method_clean" %in% names(df_working)) {
  df_working <- df_working %>%
    filter(!is.na(payment_method_clean))
} else if(all(c("percentage_of_cash", "percentage_of_stock") %in% names(df_working))) {
  df_working <- df_working %>%
    filter(!is.na(percentage_of_cash) | !is.na(percentage_of_stock))
}

n_after <- nrow(df_working)
add_step(5, "Payment method present", n_before, n_after)
log_msg(glue("  Removed: {n_before - n_after}  |  Remaining: {n_after}"))

# --------------------------------------------------------------------------
# FILTER 6: Deal value ≥ $10M
# --------------------------------------------------------------------------
log_msg("")
log_msg("FILTER 6: Deal value ≥ $10M...")

n_before <- nrow(df_working)

if("deal_value_usd_millions" %in% names(df_working)) {
  df_working <- df_working %>%
    filter(!is.na(deal_value_usd_millions), deal_value_usd_millions >= 10)
}

n_after <- nrow(df_working)
add_step(6, "Deal value ≥ $10M", n_before, n_after)
log_msg(glue("  Removed: {n_before - n_after}  |  Remaining: {n_after}"))

# --------------------------------------------------------------------------
# FILTER 7: Period 2006-2023
# --------------------------------------------------------------------------
log_msg("")
log_msg("FILTER 7: Period 2006-2023...")

n_before <- nrow(df_working)

if("year_announced" %in% names(df_working)) {
  df_working <- df_working %>%
    filter(year_announced >= 2006, year_announced <= 2023)
}

n_after <- nrow(df_working)
add_step(7, "Period 2006-2023", n_before, n_after)
log_msg(glue("  Removed: {n_before - n_after}  |  Remaining: {n_after}"))

# =============================================================================
# STEP 4: ADD target_cik COLUMN FOR MANUAL MATCHING
# =============================================================================

log_msg("")
log_msg("STEP 4: Preparing for manual CIK matching...")
log_msg(paste(rep("-", 70), collapse = ""))

if(!("target_cik" %in% names(df_working))) {
  df_working <- df_working %>%
    mutate(target_cik = NA_character_)
  log_msg("Added target_cik column (empty)")
}

# Sort by target_name for easier manual matching
df_working <- df_working %>%
  arrange(target_name)

log_msg("Sorted by target_name")

# =============================================================================
# STEP 5: SAVE OUTPUTS
# =============================================================================

log_msg("")
log_msg("STEP 5: Saving outputs...")
log_msg(paste(rep("-", 70), collapse = ""))

# 1. Save RDS
output_rds <- "data/processed/deals_restricted.rds"
saveRDS(df_working, output_rds)
log_msg(glue("✓ Saved: {output_rds}"))
log_msg(glue("  Rows: {nrow(df_working)}  |  Columns: {ncol(df_working)}"))

# 2. Save Excel (EXACT same columns, same order)
output_xlsx <- "data/processed/deals_restricted.xlsx"
write.xlsx(df_working, output_xlsx, overwrite = TRUE)
log_msg(glue("✓ Saved: {output_xlsx}"))
log_msg(glue("  Format: Excel (for manual CIK matching)"))
log_msg(glue("  Columns: {ncol(df_working)} (same as RDS)"))

# 3. Save cascade report
cascade_csv <- "data/interim/sample_cascade_strict.csv"
write.csv(cascade, cascade_csv, row.names = FALSE)
log_msg(glue("✓ Saved: {cascade_csv}"))

# =============================================================================
# STEP 6: FINAL SUMMARY
# =============================================================================

log_msg("")
log_msg(paste(rep("=", 70), collapse = ""))
log_msg("STRICT RESTRICTION CASCADE COMPLETE")
log_msg(paste(rep("=", 70), collapse = ""))

log_msg("")
log_msg("Cascade summary:")
for(i in 2:nrow(cascade)) {
  log_msg(glue("  F{cascade$step[i]}: {cascade$description[i]}"))
  log_msg(glue("      Removed: {cascade$n_removed[i]}  |  Retained: {cascade$pct_retained[i]}%"))
}

log_msg("")
log_msg(glue("Final sample: {nrow(df_working)} deals ({round(100*nrow(df_working)/n_start,1)}% retention)"))

log_msg("")
log_msg("Quality checks:")
if("deal_value_usd_millions" %in% names(df_working)) {
  log_msg(glue("  Deal value - Min: ${round(min(df_working$deal_value_usd_millions, na.rm=T),1)}M"))
  log_msg(glue("  Deal value - Median: ${round(median(df_working$deal_value_usd_millions, na.rm=T),1)}M"))
}
if("year_announced" %in% names(df_working)) {
  log_msg(glue("  Year range: {min(df_working$year_announced, na.rm=T)} - {max(df_working$year_announced, na.rm=T)}"))
}

close(log_conn)

# =============================================================================
# CONSOLE SUMMARY
# =============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║        STRICT SAMPLE RESTRICTIONS - COMPLETE                     ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Initial sample:       %-42d ║\n", n_start))
cat(sprintf("║ Final sample:         %-42d ║\n", nrow(df_working)))
cat(sprintf("║ Retention:            %.1f%%%36s ║\n", 100*nrow(df_working)/n_start, ""))
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ Filters applied:                                                 ║\n")
for(i in 2:nrow(cascade)) {
  cat(sprintf("║ F%d. %-44s -%5d ║\n",
              as.integer(cascade$step[i]),
              substr(cascade$description[i], 1, 44),
              cascade$n_removed[i]))
}
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ Outputs:                                                         ║\n")
cat("║   • deals_restricted.rds   (for R pipeline)                      ║\n")
cat("║   • deals_restricted.xlsx  (for manual CIK matching)             ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ Next steps:                                                      ║\n")
cat("║ 1. Open deals_restricted.xlsx in Excel                           ║\n")
cat("║ 2. Fill column 'target_cik' manually                             ║\n")
cat("║ 3. Save and re-import to R:                                      ║\n")
cat("║    deals <- readxl::read_xlsx('deals_restricted.xlsx')           ║\n")
cat("║    saveRDS(deals, 'deals_restricted.rds')                        ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")
cat("\n")

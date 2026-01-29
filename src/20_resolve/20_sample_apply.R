# =============================================================================
# src/20_resolve/20_sample_apply.R
# =============================================================================
# STEP 1: Apply Research Design Sample Restrictions
#
# This script applies the sample filters defined in the research design:
#   - Terminal outcome (completed OR withdrawn)
#   - Required identifiers (deal_id, target_name, announce_date)
#   - US-listed targets (implicit via data source)
#   - Valid deal type (acquisitions only, not minority stakes)
#
# Input:  data/interim/deals_ingested.rds
# Output: data/interim/deals_sample.rds
#         data/interim/sample_apply_log.txt
#
# Usage:  source("src/20_resolve/20_sample_apply.R")
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(glue)
  library(jsonlite)
})

# =============================================================================
# SETUP
# =============================================================================

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("STEP 1: APPLY SAMPLE RESTRICTIONS\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\n")

# Logging
log_file <- "data/interim/sample_apply_log.txt"
log_conn <- file(log_file, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  cat(full_msg, "\n")
}

log_msg("Sample restriction process started")

# =============================================================================
# LOAD DATA
# =============================================================================

input_file <- "data/interim/deals_ingested.rds"

if (!file.exists(input_file)) {
  log_msg("ERROR: Input file not found!", "ERROR")
  log_msg("Run ingestion first: source('src/10_ingest/ingest_deals_final.R')", "ERROR")
  close(log_conn)
  stop("Missing input: ", input_file)
}

deals <- readRDS(input_file)
n_initial <- nrow(deals)
log_msg(glue("Loaded {n_initial} deals from {input_file}"))

# =============================================================================
# SAMPLE RESTRICTION CASCADE
# =============================================================================

cascade <- tibble(
 step = character(),
  description = character(),
  n_before = integer(),
  n_after = integer(),
  n_dropped = integer()
)

add_cascade <- function(step_name, desc, n_before, n_after) {
  cascade <<- bind_rows(cascade, tibble(
    step = step_name,
    description = desc,
    n_before = n_before,
    n_after = n_after,
    n_dropped = n_before - n_after
  ))
}

# -----------------------------------------------------------------------------
# Filter 1: Has deal_id
# -----------------------------------------------------------------------------

log_msg("")
log_msg("FILTER 1: Deal identifier", "INFO")
n_before <- nrow(deals)
deals <- deals %>% filter(!is.na(deal_id) & deal_id != "")
n_after <- nrow(deals)
add_cascade("F1_deal_id", "Has valid deal_id", n_before, n_after)
log_msg(glue("  Before: {n_before} → After: {n_after} (dropped {n_before - n_after})"))

# -----------------------------------------------------------------------------
# Filter 2: Has target_name
# -----------------------------------------------------------------------------

log_msg("")
log_msg("FILTER 2: Target name", "INFO")
n_before <- nrow(deals)
deals <- deals %>% filter(!is.na(target_name) & target_name != "")
n_after <- nrow(deals)
add_cascade("F2_target_name", "Has valid target_name", n_before, n_after)
log_msg(glue("  Before: {n_before} → After: {n_after} (dropped {n_before - n_after})"))

# -----------------------------------------------------------------------------
# Filter 3: Has announcement date
# -----------------------------------------------------------------------------

log_msg("")
log_msg("FILTER 3: Announcement date", "INFO")
n_before <- nrow(deals)

# Find the announcement date column
announce_col <- names(deals)[str_detect(names(deals), "date.*announced")][1]
if (is.na(announce_col)) {
  log_msg("ERROR: Cannot find announcement date column", "ERROR")
  close(log_conn)
  stop("Missing announcement date column")
}

deals <- deals %>% filter(!is.na(.data[[announce_col]]))
n_after <- nrow(deals)
add_cascade("F3_announce_date", "Has valid announcement date", n_before, n_after)
log_msg(glue("  Column: {announce_col}"))
log_msg(glue("  Before: {n_before} → After: {n_after} (dropped {n_before - n_after})"))

# Standardize column name for downstream scripts
if (announce_col != "date_announced") {
  deals <- deals %>% rename(date_announced = all_of(announce_col))
  log_msg(glue("  Renamed {announce_col} → date_announced"))
}

# -----------------------------------------------------------------------------
# Filter 4: Terminal outcome (completed OR withdrawn)
# -----------------------------------------------------------------------------

log_msg("")
log_msg("FILTER 4: Terminal outcome", "INFO")
n_before <- nrow(deals)

# Check for deal_status or deal_completed
if ("deal_status" %in% names(deals)) {
  deals <- deals %>%
    mutate(
      status_clean = str_to_lower(str_trim(deal_status)),
      is_terminal = status_clean %in% c("completed", "withdrawn")
    ) %>%
    filter(is_terminal) %>%
    select(-status_clean, -is_terminal)
} else if ("deal_completed" %in% names(deals)) {
  deals <- deals %>% filter(!is.na(deal_completed))
} else {
  log_msg("WARNING: No outcome column found, skipping filter", "WARN")
}

n_after <- nrow(deals)
add_cascade("F4_terminal", "Terminal outcome (completed/withdrawn)", n_before, n_after)
log_msg(glue("  Before: {n_before} → After: {n_after} (dropped {n_before - n_after})"))

# Outcome distribution
if ("deal_completed" %in% names(deals)) {
  n_completed <- sum(deals$deal_completed == 1, na.rm = TRUE)
  n_withdrawn <- sum(deals$deal_completed == 0, na.rm = TRUE)
  log_msg(glue("  Completed: {n_completed}, Withdrawn: {n_withdrawn}"))
}

# -----------------------------------------------------------------------------
# Filter 5: Has target ticker (required for CIK resolution)
# -----------------------------------------------------------------------------

log_msg("")
log_msg("FILTER 5: Target ticker", "INFO")
n_before <- nrow(deals)

if ("target_ticker" %in% names(deals)) {
  deals <- deals %>% filter(!is.na(target_ticker) & target_ticker != "")
} else {
  log_msg("WARNING: No target_ticker column found", "WARN")
}

n_after <- nrow(deals)
add_cascade("F5_ticker", "Has valid target_ticker", n_before, n_after)
log_msg(glue("  Before: {n_before} → After: {n_after} (dropped {n_before - n_after})"))

# -----------------------------------------------------------------------------
# Filter 6: Time period (research design bounds)
# -----------------------------------------------------------------------------

log_msg("")
log_msg("FILTER 6: Time period", "INFO")
n_before <- nrow(deals)

# Research design: ensure comparability of 10-K disclosures
# Item 1A (Risk Factors) became mandatory in 2005
# We use 2006 as start year to ensure full coverage
min_year <- 2006
max_year <- 2023  # Allow time for terminal outcomes

if ("year_announced" %in% names(deals)) {
  deals <- deals %>% filter(year_announced >= min_year & year_announced <= max_year)
} else if ("date_announced" %in% names(deals)) {
  deals <- deals %>%
    mutate(year_announced = lubridate::year(date_announced)) %>%
    filter(year_announced >= min_year & year_announced <= max_year)
}

n_after <- nrow(deals)
add_cascade("F6_period", glue("Year {min_year}-{max_year}"), n_before, n_after)
log_msg(glue("  Period: {min_year} - {max_year}"))
log_msg(glue("  Before: {n_before} → After: {n_after} (dropped {n_before - n_after})"))

# =============================================================================
# SUMMARY
# =============================================================================

log_msg("")
log_msg(paste(rep("=", 70), collapse = ""))
log_msg("SAMPLE RESTRICTION SUMMARY")
log_msg(paste(rep("=", 70), collapse = ""))

for (i in seq_len(nrow(cascade))) {
  row <- cascade[i, ]
  pct <- round(100 * row$n_dropped / row$n_before, 1)
  log_msg(glue("{row$step}: {row$description}"))
  log_msg(glue("         {row$n_before} → {row$n_after} (dropped {row$n_dropped}, {pct}%)"))
}

retention_rate <- round(100 * nrow(deals) / n_initial, 1)
log_msg("")
log_msg(glue("FINAL: {n_initial} → {nrow(deals)} deals ({retention_rate}% retained)"))

# =============================================================================
# SAVE OUTPUT
# =============================================================================

output_file <- "data/interim/deals_sample.rds"
saveRDS(deals, output_file)
log_msg(glue("Saved: {output_file}"))

# Save cascade for documentation
write.csv(cascade, "data/interim/sample_cascade_applied.csv", row.names = FALSE)
log_msg("Saved: data/interim/sample_cascade_applied.csv")

# Save summary JSON
summary_stats <- list(
  timestamp = as.character(Sys.time()),
  input_file = input_file,
  output_file = output_file,
  n_initial = n_initial,
  n_final = nrow(deals),
  retention_pct = retention_rate,
  filters_applied = nrow(cascade)
)
write_json(summary_stats, "data/interim/sample_apply_summary.json", pretty = TRUE, auto_unbox = TRUE)

log_msg("")
log_msg("Sample restriction complete")
close(log_conn)

# Console summary
cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║              SAMPLE RESTRICTION COMPLETE                         ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Input:   %-56s ║\n", basename(input_file)))
cat(sprintf("║ Initial: %-56d ║\n", n_initial))
cat(sprintf("║ Final:   %-56d ║\n", nrow(deals)))
cat(sprintf("║ Retained: %-55s ║\n", paste0(retention_rate, "%")))
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ Output: data/interim/deals_sample.rds                            ║\n")
cat("║ Log:    data/interim/sample_apply_log.txt                        ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")
cat("\n")
cat("✓ Next: source('src/20_resolve/21_cik_resolve.R')\n\n")

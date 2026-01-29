# =============================================================================
# src/30_nlp/calculate_deal_premium.R
# =============================================================================
# Calculate deal premium from comprehensive ingested data
#
# Uses multiple price windows and creates robust premium measures
#
# Prerequisites:
#   - data/interim/deals_ingested.rds must exist
#   - Must have been created by read_deals_v4_intelligent_fixed.R
#
# Usage:
#   source("src/30_nlp/calculate_deal_premium.R")
#
# Output:
#   - data/interim/deals_with_premium.rds
#   - data/interim/premium_calculation_report.txt
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(glue)
})

cat("\n")
cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║            DEAL PREMIUM CALCULATION                             ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n")
cat("\n")

# =============================================================================
# STEP 1: LOAD DATA
# =============================================================================

cat("STEP 1: Loading comprehensive dataset...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

input_file <- "data/interim/deals_ingested.rds"

if (!file.exists(input_file)) {
  stop("Comprehensive dataset not found. Run read_deals_v4_intelligent_fixed.R first.")
}

df <- readRDS(input_file)
cat(sprintf("  Loaded: %d deals × %d columns\n\n", nrow(df), ncol(df)))

# =============================================================================
# STEP 2: IDENTIFY PRICE COLUMNS
# =============================================================================

cat("STEP 2: Identifying price columns...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

# Find all price columns
all_price_cols <- names(df)[str_detect(names(df), "price")]

cat(sprintf("  Found %d price-related columns:\n", length(all_price_cols)))
for (col in all_price_cols) {
  n_values <- sum(!is.na(df[[col]]))
  pct <- round(100 * n_values / nrow(df), 1)
  cat(sprintf("    - %-60s [%5d values, %5.1f%%]\n", col, n_values, pct))
}
cat("\n")

# Identify offer price
offer_price_col <- all_price_cols[str_detect(all_price_cols, "paid|offer|acquiror")][1]

# Identify target stock prices (by window)
target_1d <- all_price_cols[str_detect(all_price_cols, "1.*day.*prior")][1]
target_1w <- all_price_cols[str_detect(all_price_cols, "1.*week.*prior")][1]
target_4w <- all_price_cols[str_detect(all_price_cols, "4.*week.*prior")][1]

# Report what was found
cat("Mapping to premium components:\n")
cat(sprintf("  Offer price:      %s\n", 
            if (!is.na(offer_price_col)) offer_price_col else "NOT FOUND"))
cat(sprintf("  Target (1-day):   %s\n", 
            if (!is.na(target_1d)) target_1d else "NOT FOUND"))
cat(sprintf("  Target (1-week):  %s\n", 
            if (!is.na(target_1w)) target_1w else "NOT FOUND"))
cat(sprintf("  Target (4-week):  %s\n", 
            if (!is.na(target_4w)) target_4w else "NOT FOUND"))
cat("\n")

# =============================================================================
# STEP 3: CALCULATE PREMIUM
# =============================================================================

cat("STEP 3: Calculating deal premia...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

# Create premium variables
if (!is.na(offer_price_col) && !is.na(target_1d)) {
  df <- df %>%
    mutate(
      premium_1d = (!!sym(offer_price_col) / !!sym(target_1d)) - 1
    )
  n_1d <- sum(!is.na(df$premium_1d))
  cat(sprintf("  ✓ Calculated premium_1d: %d values (%.1f%%)\n", 
              n_1d, 100 * n_1d / nrow(df)))
}

if (!is.na(offer_price_col) && !is.na(target_1w)) {
  df <- df %>%
    mutate(
      premium_1w = (!!sym(offer_price_col) / !!sym(target_1w)) - 1
    )
  n_1w <- sum(!is.na(df$premium_1w))
  cat(sprintf("  ✓ Calculated premium_1w: %d values (%.1f%%)\n", 
              n_1w, 100 * n_1w / nrow(df)))
}

if (!is.na(offer_price_col) && !is.na(target_4w)) {
  df <- df %>%
    mutate(
      premium_4w = (!!sym(offer_price_col) / !!sym(target_4w)) - 1
    )
  n_4w <- sum(!is.na(df$premium_4w))
  cat(sprintf("  ✓ Calculated premium_4w: %d values (%.1f%%)\n", 
              n_4w, 100 * n_4w / nrow(df)))
}

# Create best-available premium
premium_cols <- c("premium_1d", "premium_1w", "premium_4w")
premium_cols_present <- premium_cols[premium_cols %in% names(df)]

if (length(premium_cols_present) > 0) {
  df <- df %>%
    mutate(
      deal_premium = coalesce(!!!syms(premium_cols_present))
    )
  n_premium <- sum(!is.na(df$deal_premium))
  cat(sprintf("  ✓ Created deal_premium (best available): %d values (%.1f%%)\n\n", 
              n_premium, 100 * n_premium / nrow(df)))
}

# =============================================================================
# STEP 4: SUMMARY STATISTICS
# =============================================================================

cat("STEP 4: Premium summary statistics...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

if ("deal_premium" %in% names(df)) {
  
  premium_summary <- df %>%
    filter(!is.na(deal_premium)) %>%
    summarise(
      n = n(),
      mean = mean(deal_premium, na.rm = TRUE),
      median = median(deal_premium, na.rm = TRUE),
      sd = sd(deal_premium, na.rm = TRUE),
      min = min(deal_premium, na.rm = TRUE),
      p25 = quantile(deal_premium, 0.25, na.rm = TRUE),
      p75 = quantile(deal_premium, 0.75, na.rm = TRUE),
      max = max(deal_premium, na.rm = TRUE)
    )
  
  cat(sprintf("  Observations:  %d\n", premium_summary$n))
  cat(sprintf("  Mean:          %.2f%% (%.3f)\n", 100 * premium_summary$mean, premium_summary$mean))
  cat(sprintf("  Median:        %.2f%% (%.3f)\n", 100 * premium_summary$median, premium_summary$median))
  cat(sprintf("  Std Dev:       %.2f%% (%.3f)\n", 100 * premium_summary$sd, premium_summary$sd))
  cat(sprintf("  Min:           %.2f%% (%.3f)\n", 100 * premium_summary$min, premium_summary$min))
  cat(sprintf("  25th pct:      %.2f%% (%.3f)\n", 100 * premium_summary$p25, premium_summary$p25))
  cat(sprintf("  75th pct:      %.2f%% (%.3f)\n", 100 * premium_summary$p75, premium_summary$p75))
  cat(sprintf("  Max:           %.2f%% (%.3f)\n\n", 100 * premium_summary$max, premium_summary$max))
  
  # Check for outliers
  n_negative <- sum(df$deal_premium < 0, na.rm = TRUE)
  n_extreme_high <- sum(df$deal_premium > 2, na.rm = TRUE)
  
  if (n_negative > 0) {
    cat(sprintf("  ⚠️  %d deals (%.1f%%) have negative premium\n", 
                n_negative, 100 * n_negative / premium_summary$n))
  }
  if (n_extreme_high > 0) {
    cat(sprintf("  ⚠️  %d deals (%.1f%%) have premium > 200%%\n", 
                n_extreme_high, 100 * n_extreme_high / premium_summary$n))
  }
  
  if (n_negative > 0 || n_extreme_high > 0) {
    cat("      (Consider winsorizing outliers for analysis)\n")
  }
}

# =============================================================================
# STEP 5: SAVE OUTPUT
# =============================================================================

cat("\n")
cat("STEP 5: Saving output...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

output_file <- "data/interim/deals_with_premium.rds"
saveRDS(df, output_file)
cat(sprintf("  ✓ Saved: %s\n", output_file))

# Save report
report_file <- "data/interim/premium_calculation_report.txt"
report_conn <- file(report_file, "w")

writeLines(paste(rep("=", 70), collapse = ""), report_conn)
writeLines("DEAL PREMIUM CALCULATION REPORT", report_conn)
writeLines(paste(rep("=", 70), collapse = ""), report_conn)
writeLines("", report_conn)
writeLines(sprintf("Date: %s", Sys.Date()), report_conn)
writeLines(sprintf("Input: %s", input_file), report_conn)
writeLines(sprintf("Output: %s", output_file), report_conn)
writeLines("", report_conn)

writeLines("PRICE COLUMNS IDENTIFIED", report_conn)
writeLines(paste(rep("-", 70), collapse = ""), report_conn)
writeLines(sprintf("Offer price:     %s", 
                   if (!is.na(offer_price_col)) offer_price_col else "NOT FOUND"),
           report_conn)
writeLines(sprintf("Target (1-day):  %s", 
                   if (!is.na(target_1d)) target_1d else "NOT FOUND"),
           report_conn)
writeLines(sprintf("Target (1-week): %s", 
                   if (!is.na(target_1w)) target_1w else "NOT FOUND"),
           report_conn)
writeLines(sprintf("Target (4-week): %s", 
                   if (!is.na(target_4w)) target_4w else "NOT FOUND"),
           report_conn)
writeLines("", report_conn)

if ("deal_premium" %in% names(df)) {
  writeLines("PREMIUM SUMMARY STATISTICS", report_conn)
  writeLines(paste(rep("-", 70), collapse = ""), report_conn)
  writeLines(sprintf("Observations:  %d / %d (%.1f%%)", 
                     premium_summary$n, nrow(df),
                     100 * premium_summary$n / nrow(df)), report_conn)
  writeLines(sprintf("Mean:          %.2f%% (%.4f)", 
                     100 * premium_summary$mean, premium_summary$mean), report_conn)
  writeLines(sprintf("Median:        %.2f%% (%.4f)", 
                     100 * premium_summary$median, premium_summary$median), report_conn)
  writeLines(sprintf("Std Dev:       %.2f%% (%.4f)", 
                     100 * premium_summary$sd, premium_summary$sd), report_conn)
  writeLines(sprintf("Min:           %.2f%% (%.4f)", 
                     100 * premium_summary$min, premium_summary$min), report_conn)
  writeLines(sprintf("25th pct:      %.2f%% (%.4f)", 
                     100 * premium_summary$p25, premium_summary$p25), report_conn)
  writeLines(sprintf("75th pct:      %.2f%% (%.4f)", 
                     100 * premium_summary$p75, premium_summary$p75), report_conn)
  writeLines(sprintf("Max:           %.2f%% (%.4f)", 
                     100 * premium_summary$max, premium_summary$max), report_conn)
}

close(report_conn)
cat(sprintf("  ✓ Saved: %s\n\n", report_file))

# =============================================================================
# FINAL SUMMARY
# =============================================================================

cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║              PREMIUM CALCULATION COMPLETE                       ║\n")
cat("╠════════════════════════════════════════════════════════════════╣\n")

if ("deal_premium" %in% names(df)) {
  cat(sprintf("║ Premium coverage:   %d / %d deals (%.1f%%)%15s ║\n", 
              premium_summary$n, nrow(df),
              100 * premium_summary$n / nrow(df), ""))
  cat(sprintf("║ Mean premium:       %.1f%%%38s ║\n", 
              100 * premium_summary$mean, ""))
  cat(sprintf("║ Median premium:     %.1f%%%38s ║\n", 
              100 * premium_summary$median, ""))
} else {
  cat("║ ⚠️  Could not calculate premium (missing price data)%10s ║\n", "")
}

cat("╠════════════════════════════════════════════════════════════════╣\n")
cat("║ Output:             data/interim/deals_with_premium.rds         ║\n")
cat("║ Report:             data/interim/premium_calculation_report.txt ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n")
cat("\n")
cat("✓ Ready for sample restriction or further analysis\n")
cat("  Next: source('src/20_clean/apply_sample_restrictions.R')\n\n")

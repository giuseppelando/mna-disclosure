# =============================================================================
# src/20_resolve/00_run_pipeline.R
# =============================================================================
# MASTER PIPELINE ORCHESTRATOR
#
# Runs the complete post-ingestion pipeline in sequence:
#   Step 1: Apply sample restrictions
#   Step 2: Resolve target CIK numbers
#   Step 3: Identify pre-announcement filings
#
# Prerequisites:
#   - Ingestion complete: data/interim/deals_ingested.rds exists
#
# Usage:
#   source("src/20_resolve/00_run_pipeline.R")
#
# =============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║                                                                  ║\n")
cat("║     M&A DISCLOSURE PROJECT - POST-INGESTION PIPELINE             ║\n")
cat("║                                                                  ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")
cat("\n")

# Check prerequisites
if (!file.exists("data/interim/deals_ingested.rds")) {
  stop("Missing prerequisite: data/interim/deals_ingested.rds\n",
       "Run ingestion first: source('src/10_ingest/ingest_deals_final.R')")
}

# Track timing
pipeline_start <- Sys.time()

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: SAMPLE RESTRICTIONS
# ═══════════════════════════════════════════════════════════════════════════

cat("\n")
cat(paste(rep("═", 70), collapse = ""), "\n")
cat("EXECUTING STEP 1: Sample Restrictions\n")
cat(paste(rep("═", 70), collapse = ""), "\n")

step1_start <- Sys.time()
source("src/20_clean/03_apply_sample_restrictions.R")
step1_time <- difftime(Sys.time(), step1_start, units = "mins")

cat(glue::glue("Step 1 completed in {round(step1_time, 1)} minutes"), "\n")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: CIK RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════

cat("\n")
cat(paste(rep("═", 70), collapse = ""), "\n")
cat("EXECUTING STEP 2: CIK Resolution\n")
cat(paste(rep("═", 70), collapse = ""), "\n")

step2_start <- Sys.time()
source("src/20_resolve/04_cik_resolve_MASTER.R")
step2_time <- difftime(Sys.time(), step2_start, units = "mins")

cat(glue::glue("Step 2 completed in {round(step2_time, 1)} minutes"), "\n")

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: FILING IDENTIFICATION
# ═══════════════════════════════════════════════════════════════════════════

cat("\n")
cat(paste(rep("═", 70), collapse = ""), "\n")
cat("EXECUTING STEP 3: Filing Identification\n")
cat(paste(rep("═", 70), collapse = ""), "\n")

step3_start <- Sys.time()
source("src/20_resolve/22_filing_identify.R")
step3_time <- difftime(Sys.time(), step3_start, units = "mins")

cat(glue::glue("Step 3 completed in {round(step3_time, 1)} minutes"), "\n")

# ═══════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ═══════════════════════════════════════════════════════════════════════════

pipeline_time <- difftime(Sys.time(), pipeline_start, units = "mins")

# Load final results for summary
deals_final <- readRDS("data/interim/deals_with_filing.rds")
n_total <- nrow(deals_final)
n_with_cik <- sum(!is.na(deals_final$target_cik))
n_with_filing <- sum(!is.na(deals_final$filing_accession))

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║              PIPELINE COMPLETE - FINAL SUMMARY                   ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Total execution time: %-43s ║\n", paste0(round(pipeline_time, 1), " minutes")))
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Deals in sample:      %-43d ║\n", n_total))
cat(sprintf("║ With CIK:             %-43s ║\n", paste0(n_with_cik, " (", round(100*n_with_cik/n_total, 1), "%)")))
cat(sprintf("║ With filing:          %-43s ║\n", paste0(n_with_filing, " (", round(100*n_with_filing/n_total, 1), "%)")))
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ KEY OUTPUTS:                                                     ║\n")
cat("║   data/interim/deals_sample.rds        (restricted sample)       ║\n")
cat("║   data/interim/deals_with_cik.rds      (with CIK)                ║\n")
cat("║   data/interim/deals_with_filing.rds   (with filing info)        ║\n")
cat("║   data/interim/deals_ready_for_download.csv                      ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ LOGS:                                                            ║\n")
cat("║   data/interim/sample_apply_log.txt                              ║\n")
cat("║   data/interim/cik_resolve_log.txt                               ║\n")
cat("║   data/interim/filing_identify_log.txt                           ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")
cat("\n")

# Next steps guidance
if (n_with_filing > 100) {
  cat("✓ Pipeline successful! You have ", n_with_filing, " deals ready for analysis.\n")
  cat("\n")
  cat("RECOMMENDED NEXT STEPS:\n")
  cat("  1. Review logs for any warnings or issues\n")
  cat("  2. Examine data/interim/cik_unmatched_deals.csv for manual fixes\n")
  cat("  3. Proceed to filing download: source('src/30_download/30_download_filings.R')\n")
} else if (n_with_filing > 0) {
  cat("⚠ Limited coverage. Consider:\n")
  cat("  1. Building historical ticker database for better CIK matching\n")
  cat("  2. Adjusting timing constraints in CONFIG\n")
  cat("  3. Manual CIK lookup for high-priority deals\n")
} else {
  cat("✗ No filings identified. Check:\n")
  cat("  1. SEC API connectivity\n")
  cat("  2. CIK resolution quality\n")
  cat("  3. Timing constraint configuration\n")
}

cat("\n")

# =============================================================================
# Project Cleanup Script
# =============================================================================
# Organizes project by deleting incorrect files and archiving superseded ones
#
# Usage: source("scripts/cleanup_project.R")
# =============================================================================

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("PROJECT CLEANUP\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# Create archive directories
cat("Creating archive directories...\n")
dir.create("src/10_ingest/_archive", recursive = TRUE, showWarnings = FALSE)
dir.create("data/interim/_archive", recursive = TRUE, showWarnings = FALSE)

# Delete files with incorrect data
cat("\nDeleting files with incorrect/obsolete data...\n")

files_to_delete <- c(
  "data/interim/deals_with_cik_enhanced.rds",  # Contains incorrect EDGAR matches
  "data/interim/deals_with_cik.rds",  # Superseded by verified version
  "data/interim/cik_mapping_log.txt",  # From superseded script
  "data/interim/cik_mapping_summary.json",  # From superseded script
  "data/interim/ticker_cik_lookup.csv",  # From superseded script
  "data/processed/analysis_ready.csv"  # Old preliminary analysis
)

deleted_count <- 0
for (file in files_to_delete) {
  if (file.exists(file)) {
    file.remove(file)
    cat(glue::glue("  ✓ Deleted: {file}"), "\n")
    deleted_count <- deleted_count + 1
  }
}
cat(glue::glue("Deleted {deleted_count} files\n"))

# Archive superseded scripts
cat("\nArchiving superseded scripts...\n")

scripts_to_archive <- c(
  "src/10_ingest/map_tickers_to_cik.R",
  "src/10_ingest/map_tickers_to_cik_refined.R",
  "src/10_ingest/test_historical_ticker_resolution.R"
)

archived_scripts <- 0
for (script in scripts_to_archive) {
  if (file.exists(script)) {
    new_path <- file.path("src/10_ingest/_archive", basename(script))
    file.rename(script, new_path)
    cat(glue::glue("  ✓ Archived: {basename(script)}"), "\n")
    archived_scripts <- archived_scripts + 1
  }
}
cat(glue::glue("Archived {archived_scripts} scripts\n"))

# Archive failed experiment outputs
cat("\nArchiving failed experiment outputs...\n")

outputs_to_archive <- c(
  "data/interim/ticker_cik_cache.rds",
  "data/interim/ticker_resolution_test_log.txt",
  "data/interim/ticker_resolution_report.txt"
)

archived_outputs <- 0
for (output in outputs_to_archive) {
  if (file.exists(output)) {
    new_path <- file.path("data/interim/_archive", basename(output))
    file.rename(output, new_path)
    cat(glue::glue("  ✓ Archived: {basename(output)}"), "\n")
    archived_outputs <- archived_outputs + 1
  }
}
cat(glue::glue("Archived {archived_outputs} output files\n"))

# Verify clean state
cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("VERIFICATION: Current Pipeline Files\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

cat("Production scripts in /src/10_ingest/:\n")
ingest_scripts <- list.files("src/10_ingest", pattern = "\\.R$", full.names = FALSE)
for (script in ingest_scripts) {
  cat(glue::glue("  ✓ {script}"), "\n")
}

cat("\nProduction scripts in /src/20_clean/:\n")
clean_scripts <- list.files("src/20_clean", pattern = "\\.R$", full.names = FALSE)
for (script in clean_scripts) {
  cat(glue::glue("  ✓ {script}"), "\n")
}

cat("\nPipeline data files in /data/interim/:\n")
interim_files <- list.files("data/interim", pattern = "deals_.*\\.rds$", full.names = FALSE)
for (file in interim_files) {
  cat(glue::glue("  ✓ {file}"), "\n")
}

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("CLEANUP COMPLETE\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("\nYour project is now organized with:\n")
cat("  - Production scripts in /src/ (clean)\n")
cat("  - Pipeline data files in /data/interim/ (3 RDS files)\n")
cat("  - Archived scripts in /src/10_ingest/_archive/\n")
cat("  - Archived outputs in /data/interim/_archive/\n")
cat("\nSee reports/07_PROJECT_CLEANUP_LOG.md for complete inventory\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# =============================================================================
# run_pipeline.R
# =============================================================================
# Root script to run the data pipeline in the correct order
# =============================================================================

rm(list = ls())
gc()

# Set project root (assumes this script is run from project root)
project_root <- getwd()

# Helper to source scripts with message
run_script <- function(path) {
  full_path <- file.path(project_root, path)
  if (!file.exists(full_path)) {
    stop(paste("Script not found:", full_path))
  }
  message("Running: ", path)
  source(full_path, local = FALSE)
}

# -----------------------------------------------------------------------------
# Pipeline execution
# -----------------------------------------------------------------------------

run_script("src/10_ingest/01_analyze_columns.R")
run_script("src/10_ingest/02_ingest_deals_final.R")
run_script("src/20_clean/03_apply_sample_restrictions.R")

message("Pipeline completed successfully.")



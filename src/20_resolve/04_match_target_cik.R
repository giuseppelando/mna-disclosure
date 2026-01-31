# ==============================================================================
# CIK Matching Script
# Purpose: Match target_name from deals_restricted to cik_key from companies
# ==============================================================================

library(tidyverse)
library(glue)
library(readxl)
library(writexl)

# ==============================================================================
# PHASE 1: READ INPUTS
# ==============================================================================

cat("\n")
cat("==============================================================================\n")
cat("PHASE 1: Reading input files\n")
cat("==============================================================================\n")

# Read deals_restricted
deals_path <- "C:/Users/giuse/Documents/GitHub/mna-disclosure/data/processed/deals_restricted.rds"
cat(glue("Reading deals_restricted from:\n  {deals_path}\n"))
deals_restricted <- readRDS(deals_path)
cat(glue("  → Loaded {nrow(deals_restricted)} rows, {ncol(deals_restricted)} columns\n"))
cat(glue("  → Unique target_name values: {n_distinct(deals_restricted$target_name)}\n"))

# Check if target_cik already exists
if ("target_cik" %in% names(deals_restricted)) {
  cat("  ⚠ Column 'target_cik' already exists - will be overwritten\n")
  deals_restricted <- deals_restricted %>% select(-target_cik)
  cat("  → Removed existing target_cik column\n")
}

# Read companies
cat("\n")
companies_path <- "C:/Users/giuse/Documents/GitHub/mna-disclosure/data/raw/companies.rds"
cat(glue("Reading companies from:\n  {companies_path}\n"))
companies <- readRDS(companies_path)
cat(glue("  → Loaded {nrow(companies)} rows, {ncol(companies)} columns\n"))
cat(glue("  → Unique name values: {n_distinct(companies$name)}\n"))
cat(glue("  → Unique cik_key values: {n_distinct(companies$cik_key)}\n"))

# Store original counts for validation
original_row_count <- nrow(deals_restricted)
original_unique_targets <- n_distinct(deals_restricted$target_name)

cat("\n")
cat("VALIDATION CHECKPOINTS:\n")
cat(glue("  → Original row count: {original_row_count} (must be preserved as unique targets)\n"))
cat(glue("  → Original unique target_name count: {original_unique_targets}\n"))
cat(glue("  → Expected to validate against: {original_row_count} rows representing unique deals\n"))

# ==============================================================================
# HELPER FUNCTION: Name Normalization
# ==============================================================================

normalize_name <- function(name) {
  name %>%
    str_to_lower() %>%                    # Convert to lowercase
    str_remove_all("[[:punct:]]") %>%     # Remove all punctuation
    str_squish() %>%                       # Remove extra whitespace
    str_trim()                             # Trim leading/trailing spaces
}

# ==============================================================================
# PHASE 2: TIER 1 MATCHING (EXACT)
# ==============================================================================

cat("\n")
cat("==============================================================================\n")
cat("PHASE 2: Tier 1 matching (exact string match)\n")
cat("==============================================================================\n")

# Prepare companies lookup for exact matching
companies_exact <- companies %>%
  select(name, cik_key) %>%
  distinct()

# Perform exact match
tier1_matches <- deals_restricted %>%
  left_join(
    companies_exact,
    by = c("target_name" = "name"),
    relationship = "many-to-many"
  ) %>%
  rename(target_cik = cik_key)

# Count matches
tier1_matched_count <- tier1_matches %>%
  filter(!is.na(target_cik)) %>%
  pull(target_name) %>%
  n_distinct()

tier1_row_count <- tier1_matches %>%
  filter(!is.na(target_cik)) %>%
  nrow()

tier1_unmatched_count <- tier1_matches %>%
  filter(is.na(target_cik)) %>%
  pull(target_name) %>%
  n_distinct()

cat(glue("Results:\n"))
cat(glue("  → Unique targets matched: {tier1_matched_count}\n"))
cat(glue("  → Unique targets unmatched: {tier1_unmatched_count}\n"))
cat(glue("  → Total rows with matches: {tier1_row_count}\n"))
cat(glue("     (includes duplicates for multiple CIK matches)\n"))

# ==============================================================================
# PHASE 3: TIER 2 MATCHING (NORMALIZED)
# ==============================================================================

cat("\n")
cat("==============================================================================\n")
cat("PHASE 3: Tier 2 matching (normalized names)\n")
cat("==============================================================================\n")

# Identify unmatched rows from Tier 1
tier1_unmatched <- tier1_matches %>%
  filter(is.na(target_cik))

cat(glue("Unmatched from Tier 1:\n"))
cat(glue("  → Rows requiring Tier 2 matching: {nrow(tier1_unmatched)}\n"))
cat(glue("  → Unique targets requiring Tier 2: {n_distinct(tier1_unmatched$target_name)}\n"))

if (nrow(tier1_unmatched) > 0) {
  
  cat("\n")
  cat("Applying normalized matching...\n")
  
  # Create normalized lookup tables
  deals_normalized <- tier1_unmatched %>%
    select(target_name) %>%
    distinct() %>%
    mutate(target_name_norm = normalize_name(target_name))
  
  companies_normalized <- companies %>%
    select(name, cik_key) %>%
    distinct() %>%
    mutate(name_norm = normalize_name(name))
  
  # Perform normalized match
  tier2_matches_lookup <- deals_normalized %>%
    left_join(
      companies_normalized,
      by = c("target_name_norm" = "name_norm"),
      relationship = "many-to-many"
    ) %>%
    select(target_name, cik_key) %>%
    filter(!is.na(cik_key))
  
  # Apply Tier 2 matches to unmatched rows
  tier2_matched <- tier1_unmatched %>%
    select(-target_cik) %>%
    left_join(
      tier2_matches_lookup,
      by = "target_name",
      relationship = "many-to-many"
    ) %>%
    rename(target_cik = cik_key)
  
  # Count Tier 2 matches
  tier2_matched_count <- tier2_matched %>%
    filter(!is.na(target_cik)) %>%
    pull(target_name) %>%
    n_distinct()
  
  tier2_row_count <- tier2_matched %>%
    filter(!is.na(target_cik)) %>%
    nrow()
  
  tier2_still_unmatched_count <- tier2_matched %>%
    filter(is.na(target_cik)) %>%
    pull(target_name) %>%
    n_distinct()
  
  cat(glue("\nResults:\n"))
  cat(glue("  → Unique targets matched in Tier 2: {tier2_matched_count}\n"))
  cat(glue("  → Unique targets still unmatched: {tier2_still_unmatched_count}\n"))
  cat(glue("  → Total rows with Tier 2 matches: {tier2_row_count}\n"))
  
} else {
  # No unmatched rows, create empty tier2_matched
  tier2_matched <- tier1_unmatched
  cat("\n")
  cat("No Tier 2 matching needed (all matched in Tier 1)\n")
}

# ==============================================================================
# PHASE 4: COMBINE RESULTS
# ==============================================================================

cat("\n")
cat("==============================================================================\n")
cat("PHASE 4: Combining Tier 1 and Tier 2 results\n")
cat("==============================================================================\n")

# Combine Tier 1 matches and Tier 2 matches
final_dataset <- bind_rows(
  tier1_matches %>% filter(!is.na(target_cik)),  # Tier 1 successes
  tier2_matched %>% filter(!is.na(target_cik)),  # Tier 2 successes
  tier2_matched %>% filter(is.na(target_cik))    # Final unmatched
)

cat(glue("Combined results:\n"))
cat(glue("  → Total rows in final dataset: {nrow(final_dataset)}\n"))
cat(glue("  → Rows with CIK populated: {sum(!is.na(final_dataset$target_cik))}\n"))
cat(glue("  → Rows with CIK blank: {sum(is.na(final_dataset$target_cik))}\n"))

# ==============================================================================
# PHASE 5: VALIDATION
# ==============================================================================

cat("\n")
cat("==============================================================================\n")
cat("PHASE 5: Validation\n")
cat("==============================================================================\n")

final_unique_targets <- n_distinct(final_dataset$target_name)

cat(glue("Validation checks:\n"))
cat(glue("  → Unique target_name in final dataset: {final_unique_targets}\n"))
cat(glue("  → Expected (from original data): {original_unique_targets}\n"))

if (final_unique_targets == original_unique_targets) {
  cat("  ✓ VALIDATION PASSED: All original unique targets preserved\n")
} else {
  cat("  ✗ VALIDATION FAILED: Unique target count mismatch!\n")
  cat(glue("  → Difference: {final_unique_targets - original_unique_targets}\n"))
  stop("Validation failed: unique target count does not match original")
}

cat("\n")
cat(glue("Row count check:\n"))
cat(glue("  → Original deals_restricted rows: {original_row_count}\n"))
cat(glue("  → Final dataset rows: {nrow(final_dataset)}\n"))
cat(glue("  → Additional rows (due to multiple CIK matches): {nrow(final_dataset) - original_row_count}\n"))

if (original_row_count == 3440) {
  cat("  ✓ Original row count is 3440 as expected\n")
} else {
  cat(glue("  ⚠ Original row count is {original_row_count}, not 3440\n"))
}

# ==============================================================================
# PHASE 6: SUMMARY STATISTICS
# ==============================================================================

cat("\n")
cat("==============================================================================\n")
cat("PHASE 6: Summary Statistics\n")
cat("==============================================================================\n")

total_unique <- n_distinct(final_dataset$target_name)
matched_unique <- final_dataset %>% 
  filter(!is.na(target_cik)) %>% 
  pull(target_name) %>% 
  n_distinct()
unmatched_unique <- final_dataset %>% 
  filter(is.na(target_cik)) %>% 
  pull(target_name) %>% 
  n_distinct()

cat(glue("Overall coverage:\n"))
cat(glue("  → Total unique targets: {total_unique}\n"))
cat(glue("  → Targets with at least one CIK: {matched_unique} ({round(100*matched_unique/total_unique, 1)}%)\n"))
cat(glue("  → Targets with no CIK: {unmatched_unique} ({round(100*unmatched_unique/total_unique, 1)}%)\n"))

cat("\n")
cat(glue("Row expansion:\n"))
cat(glue("  → Total rows (including duplicates): {nrow(final_dataset)}\n"))
cat(glue("  → Original rows: {original_row_count}\n"))
cat(glue("  → Expansion factor: {round(nrow(final_dataset)/original_row_count, 3)}x\n"))

# Calculate distribution of multiple matches
matched_targets <- final_dataset %>% 
  filter(!is.na(target_cik)) %>% 
  group_by(target_name) %>% 
  summarise(n_ciks = n(), .groups = "drop")

if (nrow(matched_targets) > 0) {
  avg_ciks <- mean(matched_targets$n_ciks)
  multi_match_count <- sum(matched_targets$n_ciks > 1)
  max_matches <- max(matched_targets$n_ciks)
  
  cat("\n")
  cat(glue("Multiple CIK matches:\n"))
  cat(glue("  → Average CIKs per matched target: {round(avg_ciks, 2)}\n"))
  cat(glue("  → Targets with multiple CIKs: {multi_match_count} ({round(100*multi_match_count/nrow(matched_targets), 1)}%)\n"))
  cat(glue("  → Maximum CIKs for a single target: {max_matches}\n"))
}

# ==============================================================================
# PHASE 7: EXPORT TO EXCEL
# ==============================================================================

cat("\n")
cat("==============================================================================\n")
cat("PHASE 7: Exporting to Excel\n")
cat("==============================================================================\n")

output_path <- "C:/Users/giuse/Documents/GitHub/mna-disclosure/data/interim/deals_with_cik_matches.xlsx"
cat(glue("Output file:\n  {output_path}\n"))

# Ensure output directory exists
output_dir <- dirname(output_path)
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
  cat(glue("  → Created directory: {output_dir}\n"))
}

# Export to Excel
write_xlsx(final_dataset, output_path)
file_size_mb <- file.size(output_path) / 1024 / 1024
cat(glue("\n  ✓ Export complete\n"))
cat(glue("  → File size: {round(file_size_mb, 2)} MB\n"))

# ==============================================================================
# COMPLETION
# ==============================================================================

cat("\n")
cat("==============================================================================\n")
cat("SCRIPT COMPLETED SUCCESSFULLY\n")
cat("==============================================================================\n")
cat(glue("Output: {output_path}\n"))
cat(glue("Total rows: {nrow(final_dataset)}\n"))
cat(glue("Unique targets: {n_distinct(final_dataset$target_name)}\n"))
cat(glue("Coverage: {matched_unique}/{total_unique} targets matched ({round(100*matched_unique/total_unique, 1)}%)\n"))
cat("\n")
cat("Ready for manual review and editing in Excel\n")
cat("==============================================================================\n")
cat("\n")
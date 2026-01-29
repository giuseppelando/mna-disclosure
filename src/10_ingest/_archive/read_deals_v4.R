# =============================================================================
# src/10_ingest/read_deals_v4_intelligent_fixed.R
# =============================================================================
# INTELLIGENT COMPREHENSIVE M&A DATA INGESTION (COMPLETELY FIXED)
#
# Handles duplicate column mappings by keeping only the best one
#
# Usage:
#   source("src/10_ingest/read_deals_v4_intelligent_fixed.R")
# =============================================================================

suppressPackageStartupMessages({
  library(readxl)
  library(dplyr)
  library(stringr)
  library(lubridate)
  library(tidyr)
  library(janitor)
  library(glue)
  library(jsonlite)
})

# =============================================================================
# SETUP
# =============================================================================

log_file <- "data/interim/ingestion_comprehensive_log.txt"
dir.create("data/interim", recursive = TRUE, showWarnings = FALSE)
log_conn <- file(log_file, "w")

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  full_msg <- glue("[{timestamp}] [{level}] {msg}")
  writeLines(full_msg, log_conn)
  message(full_msg)
}

log_msg(paste(rep("=", 70), collapse = ""), "INFO")
log_msg("INTELLIGENT COMPREHENSIVE M&A DATA INGESTION", "INFO")
log_msg(paste(rep("=", 70), collapse = ""), "INFO")

# =============================================================================
# STEP 1: LOAD MAPPING RESULTS
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 1: Loading column analysis and mapping...", "INFO")
log_msg(paste(rep("-", 70), collapse = ""), "INFO")

mapping_file <- "data/interim/research_mapping.csv"

if (!file.exists(mapping_file)) {
  log_msg("ERROR: Mapping file not found!", "ERROR")
  log_msg("Please run: source('src/10_ingest/analyze_and_map_columns_v2.R')", "ERROR")
  stop("Run analyze_and_map_columns_v2.R first")
}

research_mapping <- read.csv(mapping_file, stringsAsFactors = FALSE)
log_msg(glue("Loaded mapping for {nrow(research_mapping)} columns"), "INFO")

# Identify columns to keep
columns_to_keep <- research_mapping %>%
  filter(
    construct != "unmapped" |
    (construct == "unmapped" & pct_missing < 50)
  ) %>%
  arrange(
    desc(priority == "CRITICAL"),
    desc(priority == "HIGH"),
    desc(priority == "MEDIUM"),
    pct_missing
  )

log_msg(glue("Will keep {nrow(columns_to_keep)}/{nrow(research_mapping)} columns"), "INFO")

# =============================================================================
# STEP 2: LOAD RAW DATA
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 2: Loading raw data file...", "INFO")
log_msg(paste(rep("-", 70), collapse = ""), "INFO")

raw_files <- list.files("data/raw", pattern = "\\.xlsx$", full.names = TRUE)
if (length(raw_files) == 0) {
  stop("No .xlsx files found in data/raw/")
}

raw_path <- raw_files[1]
log_msg(glue("Input: {basename(raw_path)}"), "INFO")

df_raw <- read_excel(raw_path, sheet = 1, guess_max = 10000)
log_msg(glue("Loaded: {nrow(df_raw)} rows × {ncol(df_raw)} columns"), "INFO")

df_clean <- clean_names(df_raw)

# =============================================================================
# STEP 3: SELECT AND RENAME COLUMNS (SIMPLE APPROACH - NO DUPLICATES)
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 3: Selecting and renaming columns...", "INFO")
log_msg(paste(rep("-", 70), collapse = ""), "INFO")

# Simple standardization function
standardize_name <- function(col_name, construct) {
  
  # Only rename truly critical columns
  if (str_detect(construct, "identifier_deal")) return("deal_id")
  if (str_detect(construct, "identifier_target") && str_detect(col_name, "full.*name")) return("target_name")
  if (str_detect(construct, "identifier_target") && str_detect(col_name, "ticker")) return("target_ticker")
  if (str_detect(construct, "identifier_target") && str_detect(col_name, "cusip")) return("target_cusip")
  if (str_detect(construct, "identifier_acquirer") && str_detect(col_name, "full.*name")) return("acquirer_name")
  
  if (str_detect(construct, "outcome_completion") && str_detect(col_name, "^deal.*status")) return("deal_status")
  if (str_detect(construct, "control_deal_type") && str_detect(col_name, "^deal.*type")) return("deal_type")
  
  # For dates and everything else, KEEP ORIGINAL NAME
  # This avoids all duplicate name issues
  return(col_name)
}

# Apply simple mapping
name_mapping <- columns_to_keep %>%
  mutate(
    standard_name = map2_chr(column_name, construct, standardize_name),
    original_name = column_name
  ) %>%
  select(original_name, standard_name, construct, priority, pct_missing)

# Check for duplicates
dup_check <- name_mapping %>%
  count(standard_name) %>%
  filter(n > 1)

if (nrow(dup_check) > 0) {
  log_msg(glue("Found {nrow(dup_check)} duplicates - keeping best"), "WARN")
  
  # For each duplicate, keep only the best one
  for (dup_name in dup_check$standard_name) {
    # Get rows with this standard name
    dup_rows <- name_mapping %>%
      filter(standard_name == dup_name) %>%
      arrange(desc(priority == "CRITICAL"), desc(priority == "HIGH"), pct_missing)
    
    # Keep first (best), drop others
    best_original <- dup_rows$original_name[1]
    log_msg(glue("  {dup_name}: keeping {best_original}"), "DEBUG")
    
    # Mark others for removal
    name_mapping <- name_mapping %>%
      mutate(
        keep = !(standard_name == dup_name & original_name != best_original)
      )
  }
  
  name_mapping <- name_mapping %>%
    filter(keep) %>%
    select(-keep)
}

# Select columns
df_selected <- df_clean %>%
  select(all_of(name_mapping$original_name))

# Apply names
names(df_selected) <- name_mapping$standard_name

log_msg(glue("Selected {ncol(df_selected)} columns"), "INFO")
log_msg(glue("Renamed {sum(name_mapping$original_name != name_mapping$standard_name)} columns"), "INFO")

# =============================================================================
# STEP 4: DATA TYPE CONVERSION
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 4: Converting data types...", "INFO")
log_msg(paste(rep("-", 70), collapse = ""), "INFO")

parse_flexible_date <- function(x) {
  formats <- c("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d", 
               "%d-%b-%Y", "%d-%B-%Y", "%Y/%m/%d")
  
  for (fmt in formats) {
    result <- suppressWarnings(as.Date(as.character(x), format = fmt))
    if (sum(!is.na(result)) > length(result) * 0.5) {
      return(result)
    }
  }
  suppressWarnings(lubridate::as_date(x))
}

# Convert dates - look for "date" in name
date_cols <- names(df_selected)[str_detect(names(df_selected), "date|Date")]

for (col in date_cols) {
  if (!inherits(df_selected[[col]], "Date")) {
    df_selected[[col]] <- parse_flexible_date(df_selected[[col]])
    n_parsed <- sum(!is.na(df_selected[[col]]))
    log_msg(glue("  {col}: {n_parsed}/{nrow(df_selected)} dates parsed"), "DEBUG")
  }
}

# Convert numeric columns
numeric_patterns <- c(
  "value", "price", "percent", "stake", "shares", "outstanding"
)

for (col in names(df_selected)) {
  col_lower <- str_to_lower(col)
  if (any(str_detect(col_lower, numeric_patterns))) {
    if (!inherits(df_selected[[col]], c("Date", "numeric", "integer"))) {
      df_selected[[col]] <- suppressWarnings(as.numeric(df_selected[[col]]))
      n_numeric <- sum(!is.na(df_selected[[col]]))
      if (n_numeric > 0) {
        log_msg(glue("  {col}: converted to numeric ({n_numeric} values)"), "DEBUG")
      }
    }
  }
}

# Trim character columns
char_cols <- names(df_selected)[sapply(df_selected, is.character)]
for (col in char_cols) {
  df_selected[[col]] <- str_trim(df_selected[[col]])
}

log_msg("Type conversion complete", "INFO")

# =============================================================================
# STEP 5: REMOVE DUPLICATES
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 5: Deduplication...", "INFO")
log_msg(paste(rep("-", 70), collapse = ""), "INFO")

n_before <- nrow(df_selected)

if ("deal_id" %in% names(df_selected)) {
  df_selected <- df_selected %>% distinct(deal_id, .keep_all = TRUE)
} else {
  df_selected <- df_selected %>% distinct()
}

n_after <- nrow(df_selected)
n_dupes <- n_before - n_after

log_msg(glue("Removed {n_dupes} duplicates"), if (n_dupes > 0) "WARN" else "INFO")
log_msg(glue("Final: {n_after} unique deals"), "INFO")

# =============================================================================
# STEP 6: CREATE DERIVED VARIABLES
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 6: Creating derived variables...", "INFO")
log_msg(paste(rep("-", 70), collapse = ""), "INFO")

# Find relevant columns dynamically
announce_col <- names(df_selected)[str_detect(names(df_selected), "date.*announced")][1]
complete_col <- names(df_selected)[str_detect(names(df_selected), "date.*effective")][1]
withdrawn_col <- names(df_selected)[str_detect(names(df_selected), "date.*withdrawn")][1]
status_col <- names(df_selected)[str_detect(names(df_selected), "deal.*status")][1]
sic_col <- names(df_selected)[str_detect(names(df_selected), "sic.*code")][1]

# Deal outcomes
if (!is.na(status_col) && status_col %in% names(df_selected)) {
  df_selected <- df_selected %>%
    mutate(
      deal_completed = case_when(
        str_to_lower(str_trim(.data[[status_col]])) == "completed" ~ 1L,
        str_to_lower(str_trim(.data[[status_col]])) == "withdrawn" ~ 0L,
        TRUE ~ NA_integer_
      ),
      
      deal_outcome_terminal = str_to_lower(str_trim(.data[[status_col]])) %in% 
        c("completed", "withdrawn")
    )
  log_msg("Created: deal_completed, deal_outcome_terminal", "DEBUG")
}

# Time to close - SIMPLE VERSION
if (!is.na(announce_col) && announce_col %in% names(df_selected)) {
  if (!is.na(complete_col) && complete_col %in% names(df_selected)) {
    df_selected <- df_selected %>%
      mutate(
        time_to_close_complete = as.integer(.data[[complete_col]] - .data[[announce_col]])
      )
  }
  if (!is.na(withdrawn_col) && withdrawn_col %in% names(df_selected)) {
    df_selected <- df_selected %>%
      mutate(
        time_to_close_withdrawn = as.integer(.data[[withdrawn_col]] - .data[[announce_col]])
      )
  }
  # Combine: use completion time if available, else withdrawal time
  if ("time_to_close_complete" %in% names(df_selected) || "time_to_close_withdrawn" %in% names(df_selected)) {
    df_selected <- df_selected %>%
      mutate(
        time_to_close = coalesce(time_to_close_complete, time_to_close_withdrawn)
      ) %>%
      select(-any_of(c("time_to_close_complete", "time_to_close_withdrawn")))
    log_msg("Created: time_to_close", "DEBUG")
  }
}

# Industry classifications
if (!is.na(sic_col) && sic_col %in% names(df_selected)) {
  df_selected <- df_selected %>%
    mutate(
      target_sic_primary = str_extract(.data[[sic_col]], "^\\d{4}"),
      target_sic_2digit = str_sub(target_sic_primary, 1, 2)
    )
  log_msg("Created: target_sic_primary, target_sic_2digit", "DEBUG")
}

# Time indicators
if (!is.na(announce_col) && announce_col %in% names(df_selected)) {
  df_selected <- df_selected %>%
    mutate(
      year_announced = year(.data[[announce_col]]),
      month_announced = month(.data[[announce_col]])
    )
  log_msg("Created: year_announced, month_announced", "DEBUG")
}

# Industry × Year FE
if (all(c("target_sic_2digit", "year_announced") %in% names(df_selected))) {
  df_selected <- df_selected %>%
    mutate(
      industry_year = paste0(target_sic_2digit, "_", year_announced)
    )
  log_msg("Created: industry_year", "DEBUG")
}

# Payment method from percentages
if (all(c("percentage_of_cash", "percentage_of_stock") %in% names(df_selected))) {
  df_selected <- df_selected %>%
    mutate(
      payment_method_clean = case_when(
        percentage_of_cash == 100 ~ "Cash",
        percentage_of_stock == 100 ~ "Stock",
        percentage_of_cash > 0 & percentage_of_stock > 0 ~ "Mixed",
        percentage_of_cash + percentage_of_stock > 0 ~ "Other",
        TRUE ~ NA_character_
      )
    )
  log_msg("Created: payment_method_clean", "DEBUG")
}

log_msg(glue("Final dataset: {nrow(df_selected)} rows × {ncol(df_selected)} columns"), "INFO")

# =============================================================================
# =============================================================================
# STEP 7: CREATE SAMPLE FILTER FLAGS
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 7: Creating sample filter flags...", "INFO")
log_msg(paste(rep("-", 70), collapse = ""), "INFO")

df_selected <- df_selected %>%
  mutate(
    # Simple existence checks
    flag_has_deal_id = !is.na(deal_id),
    flag_has_target_name = if("target_name" %in% names(.)) !is.na(target_name) else FALSE,
    flag_has_announce_date = if(!is.na(announce_col)) !is.na(.data[[announce_col]]) else FALSE,
    
    flag_terminal_outcome = if("deal_outcome_terminal" %in% names(.)) {
      deal_outcome_terminal == TRUE
    } else {
      FALSE
    },
    
    flag_has_premium_inputs = {
      price_cols <- names(.)[str_detect(names(.), "price")]
      has_offer <- any(str_detect(price_cols, "paid|acquir"))
      has_target <- any(str_detect(price_cols, "target.*prior"))
      has_offer & has_target
    }
  )

flag_cols <- names(df_selected)[str_starts(names(df_selected), "flag_")]
flag_summary <- df_selected %>%
  summarise(across(all_of(flag_cols), ~sum(.x, na.rm = TRUE))) %>%
  pivot_longer(everything(), names_to = "flag", values_to = "count") %>%
  mutate(pct = round(100 * count / nrow(df_selected), 1))

log_msg("Sample filter flags created:", "INFO")
for (i in 1:nrow(flag_summary)) {
  log_msg(glue("  {flag_summary$flag[i]}: {flag_summary$count[i]} ({flag_summary$pct[i]}%)"), "DEBUG")
}

# STEP 8: SAVE OUTPUTS
# =============================================================================

log_msg("", "INFO")
log_msg("STEP 8: Saving outputs...", "INFO")
log_msg(paste(rep("-", 70), collapse = ""), "INFO")

output_path <- "data/interim/deals_ingested_comprehensive.rds"
saveRDS(df_selected, output_path)
log_msg(glue("Saved: {output_path}"), "INFO")

write.csv(name_mapping, 
          "data/interim/kept_columns_documentation.csv",
          row.names = FALSE)
log_msg("Saved: data/interim/kept_columns_documentation.csv", "INFO")

summary_stats <- list(
  processing_date = as.character(Sys.Date()),
  input_file = basename(raw_path),
  n_rows_raw = nrow(df_raw),
  n_rows_final = nrow(df_selected),
  n_columns_raw = ncol(df_raw),
  n_columns_kept = ncol(df_selected)
)

write_json(summary_stats, 
           "data/interim/ingestion_comprehensive_summary.json",
           pretty = TRUE, auto_unbox = TRUE)
log_msg("Saved: data/interim/ingestion_comprehensive_summary.json", "INFO")

# =============================================================================
# FINALIZE
# =============================================================================

log_msg("", "INFO")
log_msg(paste(rep("=", 70), collapse = ""), "INFO")
log_msg("COMPREHENSIVE INGESTION COMPLETE", "INFO")
log_msg(glue("Final dataset: {nrow(df_selected)} deals × {ncol(df_selected)} columns"), "INFO")
log_msg(paste(rep("=", 70), collapse = ""), "INFO")

close(log_conn)

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║       COMPREHENSIVE INGESTION COMPLETE - SUMMARY                 ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Input file:         %-44s ║\n", basename(raw_path)))
cat(sprintf("║ Rows processed:     %-44d ║\n", nrow(df_selected)))
cat(sprintf("║ Columns kept:       %d / %d%35s ║\n", 
            ncol(df_selected), ncol(df_raw), ""))
cat("╠══════════════════════════════════════════════════════════════════╣\n")

premium_cols <- names(df_selected)[str_detect(names(df_selected), "price")]
if (length(premium_cols) > 0) {
  cat(sprintf("║ Price columns:      %-44d ║\n", length(premium_cols)))
  cat("║ Ready for premium calculation!                                   ║\n")
}

cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat("║ Outputs:                                                          ║\n")
cat("║   - data/interim/deals_ingested_comprehensive.rds                 ║\n")
cat("║   - data/interim/kept_columns_documentation.csv                   ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")
cat("\n")
cat("✓ Next: source('src/30_nlp/calculate_deal_premium.R')\n\n")

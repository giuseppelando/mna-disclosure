# =============================================================================
# src/10_ingest/analyze_and_map_columns_v2.R
# =============================================================================
# COMPREHENSIVE COLUMN ANALYZER AND MAPPER (FIXED)
#
# Fixes:
# - Removed %R% operator (not available)
# - Enhanced payment method detection (percentage_of_cash/stock)
# - Better handling of string concatenation
#
# Usage:
#   source("src/10_ingest/analyze_and_map_columns_v2.R")
# =============================================================================

suppressPackageStartupMessages({
  library(readxl)
  library(dplyr)
  library(stringr)
  library(tidyr)
  library(janitor)
  library(purrr)
})

cat("\n")
cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║      COMPREHENSIVE COLUMN ANALYZER FOR M&A RESEARCH            ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n")
cat("\n")

# =============================================================================
# STEP 1: LOAD RAW DATA
# =============================================================================

cat("STEP 1: Loading raw data file...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

raw_files <- list.files("data/raw", pattern = "\\.xlsx$", full.names = TRUE)
if (length(raw_files) == 0) {
  stop("No .xlsx files found in data/raw/")
}

raw_path <- raw_files[1]
cat(sprintf("  Input file: %s\n", basename(raw_path)))

df_raw <- read_excel(raw_path, sheet = 1, guess_max = 10000)
cat(sprintf("  Dimensions: %d rows × %d columns\n\n", nrow(df_raw), ncol(df_raw)))

# Clean column names
df_clean <- clean_names(df_raw)

# =============================================================================
# STEP 2: ANALYZE EACH COLUMN
# =============================================================================

cat("STEP 2: Analyzing each column...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

analyze_column <- function(col_name, col_data) {
  
  n_total <- length(col_data)
  n_na <- sum(is.na(col_data))
  pct_na <- round(100 * n_na / n_total, 1)
  n_unique <- length(unique(col_data[!is.na(col_data)]))
  
  # Infer data type
  if (is.numeric(col_data)) {
    inferred_type <- "numeric"
    min_val <- min(col_data, na.rm = TRUE)
    max_val <- max(col_data, na.rm = TRUE)
    value_summary <- sprintf("Range: [%.2f, %.2f]", min_val, max_val)
  } else if (inherits(col_data, "Date") || inherits(col_data, "POSIXct")) {
    inferred_type <- "date"
    min_val <- min(col_data, na.rm = TRUE)
    max_val <- max(col_data, na.rm = TRUE)
    value_summary <- sprintf("Range: [%s, %s]", 
                             format(min_val, "%Y-%m-%d"),
                             format(max_val, "%Y-%m-%d"))
  } else {
    inferred_type <- "character"
    top_vals <- names(sort(table(col_data), decreasing = TRUE)[1:3])
    value_summary <- paste(top_vals[!is.na(top_vals)], collapse = " | ")
  }
  
  tibble(
    column_name = col_name,
    inferred_type = inferred_type,
    n_missing = n_na,
    pct_missing = pct_na,
    n_unique = n_unique,
    value_summary = value_summary
  )
}

column_analysis <- map_dfr(names(df_clean), 
                           ~analyze_column(.x, df_clean[[.x]]))

cat(sprintf("  Analyzed %d columns\n", nrow(column_analysis)))
cat(sprintf("  Numeric columns: %d\n", sum(column_analysis$inferred_type == "numeric")))
cat(sprintf("  Date columns: %d\n", sum(column_analysis$inferred_type == "date")))
cat(sprintf("  Character columns: %d\n\n", sum(column_analysis$inferred_type == "character")))

# =============================================================================
# STEP 3: INTELLIGENT MAPPING TO RESEARCH CONSTRUCTS
# =============================================================================

cat("STEP 3: Mapping columns to research constructs...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

map_to_construct <- function(col_name) {
  
  name_norm <- str_to_lower(col_name)
  
  # OUTCOMES - Premium inputs
  if (str_detect(name_norm, "offer.*price|price.*per.*share|price.*paid")) {
    return(list(construct = "outcome_premium_input", 
                priority = "CRITICAL",
                description = "Offer price (premium numerator)"))
  }
  if (str_detect(name_norm, "target.*price|stock.*price|share.*price|closing.*price")) {
    return(list(construct = "outcome_premium_input",
                priority = "CRITICAL",
                description = "Target stock price (premium denominator)"))
  }
  if (str_detect(name_norm, "shares.*outstanding|outstanding.*shares")) {
    return(list(construct = "outcome_premium_input",
                priority = "HIGH",
                description = "Shares outstanding (for premium calc)"))
  }
  if (str_detect(name_norm, "deal.*status|^status$|transaction.*status")) {
    return(list(construct = "outcome_completion",
                priority = "CRITICAL",
                description = "Deal status (completed/withdrawn)"))
  }
  
  # DATES
  if (str_detect(name_norm, "date.*announced|announced.*date|announcement")) {
    return(list(construct = "date_announcement",
                priority = "CRITICAL",
                description = "Deal announcement date"))
  }
  if (str_detect(name_norm, "date.*effective|completed.*date|completion|effective.*date")) {
    return(list(construct = "date_completion",
                priority = "CRITICAL",
                description = "Deal completion date"))
  }
  if (str_detect(name_norm, "date.*withdrawn|withdrawn.*date|termination")) {
    return(list(construct = "date_withdrawn",
                priority = "HIGH",
                description = "Deal withdrawal date"))
  }
  
  # IDENTIFIERS
  if (str_detect(name_norm, "^deal.*number|^deal.*id|^sdc")) {
    return(list(construct = "identifier_deal",
                priority = "CRITICAL",
                description = "Deal unique identifier"))
  }
  if (str_detect(name_norm, "target.*name|target.*company")) {
    return(list(construct = "identifier_target",
                priority = "CRITICAL",
                description = "Target company name"))
  }
  if (str_detect(name_norm, "target.*ticker|ticker.*symbol")) {
    return(list(construct = "identifier_target",
                priority = "HIGH",
                description = "Target ticker (for EDGAR matching)"))
  }
  if (str_detect(name_norm, "target.*cusip|^cusip")) {
    return(list(construct = "identifier_target",
                priority = "MEDIUM",
                description = "Target CUSIP"))
  }
  if (str_detect(name_norm, "acquir.*name|buyer.*name")) {
    return(list(construct = "identifier_acquirer",
                priority = "HIGH",
                description = "Acquirer company name"))
  }
  
  # PAYMENT METHOD - Enhanced detection!
  if (str_detect(name_norm, "payment.*method|consideration|method.*payment")) {
    return(list(construct = "control_payment",
                priority = "HIGH",
                description = "Payment method (cash/stock/mixed)"))
  }
  # NEW: Detect percentage components
  if (str_detect(name_norm, "percentage.*cash|percent.*cash|^pct.*cash")) {
    return(list(construct = "control_payment",
                priority = "HIGH",
                description = "Percentage of cash consideration"))
  }
  if (str_detect(name_norm, "percentage.*stock|percent.*stock|^pct.*stock|percentage.*shares")) {
    return(list(construct = "control_payment",
                priority = "HIGH",
                description = "Percentage of stock consideration"))
  }
  
  # DEAL CHARACTERISTICS
  if (str_detect(name_norm, "deal.*value|transaction.*value|rank.*value|value.*usd|value.*eur")) {
    return(list(construct = "control_deal_value",
                priority = "HIGH",
                description = "Deal value (control variable)"))
  }
  if (str_detect(name_norm, "percent.*owned.*before|initial.*stake|percent.*before")) {
    return(list(construct = "control_stake",
                priority = "HIGH",
                description = "Initial stake (toehold)"))
  }
  if (str_detect(name_norm, "percent.*acquired|final.*stake|percent.*after")) {
    return(list(construct = "control_stake",
                priority = "HIGH",
                description = "Final stake (control transfer check)"))
  }
  if (str_detect(name_norm, "deal.*type|transaction.*type|^form$")) {
    return(list(construct = "control_deal_type",
                priority = "HIGH",
                description = "Deal type (merger/acquisition)"))
  }
  if (str_detect(name_norm, "hostile|attitude|friendly")) {
    return(list(construct = "moderator_deal",
                priority = "MEDIUM",
                description = "Deal attitude (hostile/friendly)"))
  }
  if (str_detect(name_norm, "tender.*offer|tender")) {
    return(list(construct = "moderator_deal",
                priority = "MEDIUM",
                description = "Tender offer indicator"))
  }
  if (str_detect(name_norm, "competing|unsolicited")) {
    return(list(construct = "moderator_deal",
                priority = "MEDIUM",
                description = "Competition indicator"))
  }
  if (str_detect(name_norm, "acquisition.*technique|deal.*technique")) {
    return(list(construct = "moderator_deal",
                priority = "MEDIUM",
                description = "Acquisition technique"))
  }
  
  # INDUSTRY & GEOGRAPHY
  if (str_detect(name_norm, "target.*sic|sic.*code|target.*industry.*code")) {
    return(list(construct = "control_industry",
                priority = "HIGH",
                description = "Target SIC code (for FE)"))
  }
  if (str_detect(name_norm, "acquir.*sic|buyer.*sic")) {
    return(list(construct = "control_industry",
                priority = "MEDIUM",
                description = "Acquirer SIC (for relatedness)"))
  }
  if (str_detect(name_norm, "target.*nation|target.*country")) {
    return(list(construct = "control_geography",
                priority = "HIGH",
                description = "Target country (US filter)"))
  }
  if (str_detect(name_norm, "target.*state")) {
    return(list(construct = "control_geography",
                priority = "MEDIUM",
                description = "Target state"))
  }
  if (str_detect(name_norm, "target.*exchange|stock.*exchange|listed")) {
    return(list(construct = "control_geography",
                priority = "HIGH",
                description = "Target exchange (US listing check)"))
  }
  if (str_detect(name_norm, "acquir.*nation|acquir.*country|buyer.*country")) {
    return(list(construct = "moderator_geography",
                priority = "MEDIUM",
                description = "Acquirer country (cross-border)"))
  }
  if (str_detect(name_norm, "acquir.*state|buyer.*state")) {
    return(list(construct = "moderator_geography",
                priority = "MEDIUM",
                description = "Acquirer state"))
  }
  if (str_detect(name_norm, "cross.*border")) {
    return(list(construct = "moderator_geography",
                priority = "MEDIUM",
                description = "Cross-border indicator"))
  }
  if (str_detect(name_norm, "target.*macro.*industry|macro.*industry")) {
    return(list(construct = "control_industry",
                priority = "MEDIUM",
                description = "Broad industry classification"))
  }
  if (str_detect(name_norm, "target.*mid.*industry|mid.*industry")) {
    return(list(construct = "control_industry",
                priority = "MEDIUM",
                description = "Medium industry classification"))
  }
  
  # FIRM FUNDAMENTALS
  if (str_detect(name_norm, "market.*cap|market.*value")) {
    return(list(construct = "control_size",
                priority = "HIGH",
                description = "Market capitalization"))
  }
  if (str_detect(name_norm, "total.*assets|^assets$")) {
    return(list(construct = "control_size",
                priority = "HIGH",
                description = "Total assets"))
  }
  if (str_detect(name_norm, "sales|revenue|turnover")) {
    return(list(construct = "control_size",
                priority = "MEDIUM",
                description = "Sales/Revenue"))
  }
  if (str_detect(name_norm, "debt|leverage|liabilities")) {
    return(list(construct = "control_leverage",
                priority = "MEDIUM",
                description = "Debt/Leverage measure"))
  }
  
  # PUBLIC STATUS
  if (str_detect(name_norm, "public.*status|listing.*status")) {
    return(list(construct = "control_public_status",
                priority = "HIGH",
                description = "Public listing status"))
  }
  
  # RELATED DEALS
  if (str_detect(name_norm, "related.*deal|related.*sdc")) {
    return(list(construct = "moderator_related_deal",
                priority = "LOW",
                description = "Related deal identifier"))
  }
  
  # RANK DATE
  if (str_detect(name_norm, "rank.*date")) {
    return(list(construct = "metadata_rank",
                priority = "LOW",
                description = "Ranking date for deal value"))
  }
  
  # DEFAULT: UNMAPPED
  return(list(construct = "unmapped",
              priority = "LOW",
              description = "Not automatically mapped"))
}

# Apply mapping
research_mapping <- column_analysis %>%
  rowwise() %>%
  mutate(
    mapping = list(map_to_construct(column_name)),
    construct = mapping$construct,
    priority = mapping$priority,
    construct_description = mapping$description
  ) %>%
  select(-mapping) %>%
  ungroup()

# Count by construct
construct_summary <- research_mapping %>%
  filter(construct != "unmapped") %>%
  count(construct, priority, sort = TRUE)

cat("  Mapped columns by construct:\n")
for (i in 1:nrow(construct_summary)) {
  cat(sprintf("    %s [%s]: %d columns\n", 
              construct_summary$construct[i],
              construct_summary$priority[i],
              construct_summary$n[i]))
}

# =============================================================================
# STEP 4: CHECK FOR MISSING CRITICAL CONSTRUCTS
# =============================================================================

cat("\n")
cat("STEP 4: Checking for missing critical constructs...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

critical_constructs <- c(
  "outcome_premium_input",
  "outcome_completion",
  "date_announcement",
  "date_completion",
  "identifier_deal",
  "identifier_target",
  "control_deal_value",
  "control_payment",
  "control_industry"
)

present_constructs <- unique(research_mapping$construct)
missing_critical <- setdiff(critical_constructs, present_constructs)

if (length(missing_critical) > 0) {
  cat("  ⚠️  WARNING: Missing critical constructs:\n")
  for (mc in missing_critical) {
    cat(sprintf("      - %s\n", mc))
  }
} else {
  cat("  ✓ All critical constructs present\n")
}

# =============================================================================
# STEP 5: FLAG UNMAPPED COLUMNS
# =============================================================================

cat("\n")
cat("STEP 5: Identifying potentially useful unmapped columns...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

unmapped <- research_mapping %>%
  filter(construct == "unmapped") %>%
  filter(pct_missing < 90, n_unique > 1) %>%
  arrange(pct_missing)

if (nrow(unmapped) > 0) {
  cat(sprintf("  Found %d unmapped columns with <90%% missing data:\n", nrow(unmapped)))
  cat("  (These may contain useful information - review manually)\n\n")
  
  for (i in 1:min(20, nrow(unmapped))) {
    cat(sprintf("    %2d. %-50s [%5.1f%% complete]\n",
                i,
                unmapped$column_name[i],
                100 - unmapped$pct_missing[i]))
  }
  
  if (nrow(unmapped) > 20) {
    cat(sprintf("    ... and %d more (see column_analysis.csv)\n", nrow(unmapped) - 20))
  }
} else {
  cat("  ✓ All columns with useful data have been mapped\n")
}

# =============================================================================
# STEP 6: SAVE OUTPUTS
# =============================================================================

cat("\n")
cat("STEP 6: Saving analysis outputs...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

dir.create("data/interim", recursive = TRUE, showWarnings = FALSE)

# Full column analysis
write.csv(column_analysis, 
          "data/interim/column_analysis.csv", 
          row.names = FALSE)
cat("  ✓ Saved: data/interim/column_analysis.csv\n")

# Research mapping
write.csv(research_mapping,
          "data/interim/research_mapping.csv",
          row.names = FALSE)
cat("  ✓ Saved: data/interim/research_mapping.csv\n")

# Human-readable report
report_file <- "data/interim/column_analysis_report.txt"
report_conn <- file(report_file, "w")

writeLines(paste(rep("=", 70), collapse = ""), report_conn)
writeLines("COLUMN ANALYSIS REPORT FOR M&A RESEARCH", report_conn)
writeLines(paste(rep("=", 70), collapse = ""), report_conn)
writeLines("", report_conn)
writeLines(sprintf("Input file: %s", basename(raw_path)), report_conn)
writeLines(sprintf("Analysis date: %s", Sys.Date()), report_conn)
writeLines(sprintf("Total columns: %d", nrow(column_analysis)), report_conn)
writeLines("", report_conn)

writeLines("SUMMARY BY CONSTRUCT", report_conn)
writeLines(paste(rep("-", 70), collapse = ""), report_conn)
for (i in 1:nrow(construct_summary)) {
  writeLines(sprintf("%-40s [%8s]: %3d columns",
                     construct_summary$construct[i],
                     construct_summary$priority[i],
                     construct_summary$n[i]), report_conn)
}
writeLines("", report_conn)

writeLines("CRITICAL COLUMNS FOR PREMIUM CALCULATION", report_conn)
writeLines(paste(rep("-", 70), collapse = ""), report_conn)
premium_cols <- research_mapping %>%
  filter(construct == "outcome_premium_input") %>%
  arrange(desc(priority == "CRITICAL"), pct_missing)

if (nrow(premium_cols) > 0) {
  for (i in 1:nrow(premium_cols)) {
    writeLines(sprintf("  [%s] %s", 
                       premium_cols$priority[i],
                       premium_cols$column_name[i]), report_conn)
    writeLines(sprintf("      %s", premium_cols$construct_description[i]), report_conn)
    writeLines(sprintf("      Missing: %.1f%%, Unique values: %d",
                       premium_cols$pct_missing[i],
                       premium_cols$n_unique[i]), report_conn)
    writeLines("", report_conn)
  }
} else {
  writeLines("  ⚠️  NO PREMIUM INPUT COLUMNS FOUND!", report_conn)
}
writeLines("", report_conn)

writeLines("PAYMENT METHOD COLUMNS", report_conn)
writeLines(paste(rep("-", 70), collapse = ""), report_conn)
payment_cols <- research_mapping %>%
  filter(construct == "control_payment") %>%
  arrange(desc(priority == "HIGH"), pct_missing)

if (nrow(payment_cols) > 0) {
  for (i in 1:nrow(payment_cols)) {
    writeLines(sprintf("  [%s] %s",
                       payment_cols$priority[i],
                       payment_cols$column_name[i]), report_conn)
    writeLines(sprintf("      %s", payment_cols$construct_description[i]), report_conn)
    writeLines(sprintf("      Missing: %.1f%%, Unique values: %d",
                       payment_cols$pct_missing[i],
                       payment_cols$n_unique[i]), report_conn)
    writeLines("", report_conn)
  }
} else {
  writeLines("  ⚠️  NO PAYMENT METHOD COLUMNS FOUND!", report_conn)
}
writeLines("", report_conn)

writeLines("MISSING CRITICAL CONSTRUCTS", report_conn)
writeLines(paste(rep("-", 70), collapse = ""), report_conn)
if (length(missing_critical) > 0) {
  for (mc in missing_critical) {
    writeLines(sprintf("  ⚠️  %s", mc), report_conn)
  }
} else {
  writeLines("  ✓ All critical constructs present", report_conn)
}
writeLines("", report_conn)

writeLines("POTENTIALLY USEFUL UNMAPPED COLUMNS (Top 20)", report_conn)
writeLines(paste(rep("-", 70), collapse = ""), report_conn)
if (nrow(unmapped) > 0) {
  for (i in 1:min(20, nrow(unmapped))) {
    writeLines(sprintf("%2d. %-55s [%5.1f%% complete]",
                       i,
                       unmapped$column_name[i],
                       100 - unmapped$pct_missing[i]), report_conn)
  }
} else {
  writeLines("  (none)", report_conn)
}

close(report_conn)
cat("  ✓ Saved: data/interim/column_analysis_report.txt\n")

# =============================================================================
# STEP 7: GENERATE RECOMMENDATIONS
# =============================================================================

cat("\n")
cat("STEP 7: Generating recommendations...\n")
cat(paste(rep("-", 66), collapse = ""), "\n")

recommendations <- list()

# Check premium calculation feasibility
premium_inputs <- research_mapping %>%
  filter(construct == "outcome_premium_input")

if (nrow(premium_inputs) == 0) {
  recommendations <- c(recommendations, 
    "❌ CRITICAL: No premium input columns found.")
} else if (nrow(premium_inputs) < 2) {
  recommendations <- c(recommendations,
    "⚠️  WARNING: Found only 1 premium input. Need both offer price AND market price.")
} else {
  has_offer <- any(str_detect(premium_inputs$column_name, "offer.*price"))
  has_market <- any(str_detect(premium_inputs$column_name, "target.*price|stock.*price"))
  
  if (has_offer && has_market) {
    recommendations <- c(recommendations,
      "✓ GOOD: Premium calculation appears feasible with existing columns.")
  } else {
    recommendations <- c(recommendations,
      "⚠️  WARNING: Premium inputs detected but check they're the right ones.")
  }
}

# Check payment method
payment_cols <- research_mapping %>%
  filter(construct == "control_payment")

if (nrow(payment_cols) == 0) {
  recommendations <- c(recommendations,
    "❌ CRITICAL: No payment method columns found.")
} else if (nrow(payment_cols) >= 2) {
  recommendations <- c(recommendations,
    "✓ GOOD: Payment method can be constructed from percentage columns.")
} else {
  recommendations <- c(recommendations,
    "✓ GOOD: Payment method column found.")
}

# Check control variables
high_priority <- sum(research_mapping$priority == "HIGH")
if (high_priority < 10) {
  recommendations <- c(recommendations,
    sprintf("⚠️  Only %d high-priority control variables found.", high_priority))
}

# Check unmapped columns
if (nrow(unmapped) > 5) {
  recommendations <- c(recommendations,
    sprintf("💡 REVIEW: %d unmapped columns with data. Some may be useful.",
            nrow(unmapped)))
}

cat("  RECOMMENDATIONS:\n")
for (rec in recommendations) {
  cat(sprintf("    %s\n", rec))
}

# =============================================================================
# FINAL SUMMARY
# =============================================================================

cat("\n")
cat("╔════════════════════════════════════════════════════════════════╗\n")
cat("║                   ANALYSIS COMPLETE                             ║\n")
cat("╠════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Total columns analyzed:      %-33d ║\n", nrow(column_analysis)))
cat(sprintf("║ Mapped to constructs:        %-33d ║\n", sum(research_mapping$construct != "unmapped")))
cat(sprintf("║ Unmapped (>10%% data):        %-33d ║\n", nrow(unmapped)))
cat("╠════════════════════════════════════════════════════════════════╣\n")
cat("║ Priority breakdown:                                             ║\n")
cat(sprintf("║   CRITICAL:                  %-33d ║\n", sum(research_mapping$priority == "CRITICAL")))
cat(sprintf("║   HIGH:                      %-33d ║\n", sum(research_mapping$priority == "HIGH")))
cat(sprintf("║   MEDIUM:                    %-33d ║\n", sum(research_mapping$priority == "MEDIUM")))
cat("╠════════════════════════════════════════════════════════════════╣\n")
cat("║ Next steps:                                                     ║\n")
cat("║ 1. Review: data/interim/column_analysis_report.txt             ║\n")
cat("║ 2. Check: Payment method columns (percentage_of_cash/stock)    ║\n")
cat("║ 3. Run: read_deals_v4_intelligent.R                            ║\n")
cat("╚════════════════════════════════════════════════════════════════╝\n")
cat("\n")

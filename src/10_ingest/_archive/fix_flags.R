# Fix the flag creation section - same && vs & issue

lines <- readLines("src/10_ingest/read_deals_v4.R")

# Find the flag section
start_idx <- which(grepl("# STEP 7: CREATE SAMPLE FILTER FLAGS", lines))[1]
end_idx <- which(grepl("# STEP 8: SAVE OUTPUTS", lines))[1] - 1

# NEW SIMPLE FLAGS - no && operators, just simple checks
new_section <- c(
  "# =============================================================================",
  "# STEP 7: CREATE SAMPLE FILTER FLAGS",
  "# =============================================================================",
  "",
  "log_msg(\"\", \"INFO\")",
  "log_msg(\"STEP 7: Creating sample filter flags...\", \"INFO\")",
  "log_msg(paste(rep(\"-\", 70), collapse = \"\"), \"INFO\")",
  "",
  "df_selected <- df_selected %>%",
  "  mutate(",
  "    # Simple existence checks",
  "    flag_has_deal_id = !is.na(deal_id),",
  "    flag_has_target_name = if(\"target_name\" %in% names(.)) !is.na(target_name) else FALSE,",
  "    flag_has_announce_date = if(!is.na(announce_col)) !is.na(.data[[announce_col]]) else FALSE,",
  "    ",
  "    flag_terminal_outcome = if(\"deal_outcome_terminal\" %in% names(.)) {",
  "      deal_outcome_terminal == TRUE",
  "    } else {",
  "      FALSE",
  "    },",
  "    ",
  "    flag_has_premium_inputs = {",
  "      price_cols <- names(.)[str_detect(names(.), \"price\")]",
  "      has_offer <- any(str_detect(price_cols, \"paid|acquir\"))",
  "      has_target <- any(str_detect(price_cols, \"target.*prior\"))",
  "      has_offer & has_target",
  "    }",
  "  )",
  "",
  "flag_cols <- names(df_selected)[str_starts(names(df_selected), \"flag_\")]",
  "flag_summary <- df_selected %>%",
  "  summarise(across(all_of(flag_cols), ~sum(.x, na.rm = TRUE))) %>%",
  "  pivot_longer(everything(), names_to = \"flag\", values_to = \"count\") %>%",
  "  mutate(pct = round(100 * count / nrow(df_selected), 1))",
  "",
  "log_msg(\"Sample filter flags created:\", \"INFO\")",
  "for (i in 1:nrow(flag_summary)) {",
  "  log_msg(glue(\"  {flag_summary$flag[i]}: {flag_summary$count[i]} ({flag_summary$pct[i]}%)\"), \"DEBUG\")",
  "}",
  ""
)

# Rebuild
new_lines <- c(
  lines[1:(start_idx-1)],
  new_section,
  lines[(end_idx+1):length(lines)]
)

writeLines(new_lines, "src/10_ingest/read_deals_v4.R")

cat("✅ Fixed flag creation section\n")
cat("✅ Removed all && operators\n")
cat("✅ Now run: source('src/10_ingest/read_deals_v4.R')\n")
cat("\nThis should complete successfully!\n")

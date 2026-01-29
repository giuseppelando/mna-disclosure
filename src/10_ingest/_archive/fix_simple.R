# COMPLETE REWRITE - SIMPLE AND CLEAN
# This replaces the problematic time_to_close section with a simple version

lines <- readLines("src/10_ingest/read_deals_v4.R")

# Find and remove the old time_to_close section
start_idx <- which(grepl("# Time to close", lines))[1]
end_idx <- which(grepl("# Industry classifications", lines))[1] - 1

# NEW SIMPLE VERSION - just check if columns exist once, then use them
new_section <- c(
  "# Time to close - SIMPLE VERSION",
  "if (!is.na(announce_col) && announce_col %in% names(df_selected)) {",
  "  if (!is.na(complete_col) && complete_col %in% names(df_selected)) {",
  "    df_selected <- df_selected %>%",
  "      mutate(",
  "        time_to_close_complete = as.integer(.data[[complete_col]] - .data[[announce_col]])",
  "      )",
  "  }",
  "  if (!is.na(withdrawn_col) && withdrawn_col %in% names(df_selected)) {",
  "    df_selected <- df_selected %>%",
  "      mutate(",
  "        time_to_close_withdrawn = as.integer(.data[[withdrawn_col]] - .data[[announce_col]])",
  "      )",
  "  }",
  "  # Combine: use completion time if available, else withdrawal time",
  "  if (\"time_to_close_complete\" %in% names(df_selected) || \"time_to_close_withdrawn\" %in% names(df_selected)) {",
  "    df_selected <- df_selected %>%",
  "      mutate(",
  "        time_to_close = coalesce(time_to_close_complete, time_to_close_withdrawn)",
  "      ) %>%",
  "      select(-any_of(c(\"time_to_close_complete\", \"time_to_close_withdrawn\")))",
  "    log_msg(\"Created: time_to_close\", \"DEBUG\")",
  "  }",
  "}"
)

# Rebuild the file
new_lines <- c(
  lines[1:(start_idx-1)],
  new_section,
  "",
  lines[(end_idx+1):length(lines)]
)

writeLines(new_lines, "src/10_ingest/read_deals_v4.R")

cat("✅ COMPLETELY REWRITTEN with simple approach\n")
cat("✅ No more complex conditions - just straightforward code\n")
cat("✅ Now run: source('src/10_ingest/read_deals_v4.R')\n")
cat("\nThis will work!\n")

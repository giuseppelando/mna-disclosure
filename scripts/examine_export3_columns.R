# Extract column names from Export 3
# Run this to see what columns need to be mapped in data_schema.yaml

library(readxl)

# Read Export 3
raw_file <- "data/raw/Deals_v1a.xlsx"
df <- read_excel(raw_file, n_max = 3)

cat("=================================================================\n")
cat("EXPORT 3 COLUMN NAMES\n")
cat("=================================================================\n\n")

col_names <- names(df)
cat(sprintf("Total columns: %d\n\n", length(col_names)))

cat("Column listing:\n")
cat(paste0(sprintf("%2d. %-60s", seq_along(col_names), col_names), collapse = "\n"))
cat("\n\n")

cat("Sample data (first 3 rows, first 10 columns):\n")
print(df[, 1:min(10, ncol(df))])

# -----------------------------------------
# aggregate_to_deal.R
# Aggrega gli indici testuali dei filing a livello target/deal
# Input : data/interim/filings_text.csv
# Output: data/processed/analysis_ready.csv
# -----------------------------------------

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
  library(fs)
})

# -------- Paths --------
DIR_IN  <- "data/interim"
DIR_OUT <- "data/processed"
FILE_IN <- file.path(DIR_IN, "filings_text.csv")
FILE_OUT <- file.path(DIR_OUT, "analysis_ready.csv")

dir_create(DIR_OUT)

# -------- Read filings --------
fil <- read_csv(FILE_IN, show_col_types = FALSE)

# controllo minimo
stopifnot(all(c("ticker","form","filing_date","n_words") %in% names(fil)))

# Filtra solo quelli validi
fil <- fil %>%
  filter(parse_ok, n_words > 0)

# -------- Aggregazione --------
# A questo punto abbiamo più filing per lo stesso target.
# Aggrego per (ticker, form) → poi potremo unire con i deal via ticker.
# Media ponderata per n_words.

agg <- fil %>%
  group_by(ticker) %>%
  summarise(
    n_docs = n(),
    total_words = sum(n_words, na.rm = TRUE),
    avg_n_words = mean(n_words, na.rm = TRUE),
    share_10k = mean(form == "10-K"),
    share_10q = mean(form == "10-Q"),
    share_6k = mean(form == "6-K"),
    tone_mean = weighted.mean(tone_lm, n_words, na.rm = TRUE),
    forward_mean = weighted.mean(forward_density, n_words, na.rm = TRUE),
    uncertainty_mean = weighted.mean(uncertainty_density, n_words, na.rm = TRUE),
    numeric_mean = weighted.mean(numeric_specificity, n_words, na.rm = TRUE),
    has_mdna = mean(has_mdna, na.rm = TRUE),
    has_risk = mean(has_risk, na.rm = TRUE),
    has_outlook = mean(has_outlook, na.rm = TRUE),
    first_filing = min(filing_date, na.rm = TRUE),
    last_filing = max(filing_date, na.rm = TRUE)
  ) %>%
  ungroup()

# -------- Salvataggio --------
write_csv(agg, FILE_OUT)
message("✅ Saved: ", FILE_OUT, " (", nrow(agg), " righe)")

# -------- Quick QC --------
qc <- list(
  tickers = n_distinct(agg$ticker),
  mean_docs_per_ticker = mean(agg$n_docs),
  mean_forward = mean(agg$forward_mean, na.rm = TRUE),
  mean_tone = mean(agg$tone_mean, na.rm = TRUE)
)
print(qc)

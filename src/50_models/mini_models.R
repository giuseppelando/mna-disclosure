# ---------------------------------------------------------
# mini_models.R  (PATCH)
# Smoke test: Premium ~ indici testuali + controlli
# Input : data/processed/analysis_ready.csv
#         data/processed/deals_core.xlsx
# Output: output/tables/main_results.html
#         output/figures/coef_ols.png
#         output/tables/influence_diagnostics.csv
# ---------------------------------------------------------

suppressPackageStartupMessages({
  library(readr)
  library(readxl)
  library(dplyr)
  library(stringr)
  library(tidyr)
  library(fs)
  library(ggplot2)
  library(broom)
  library(lmtest)
  library(sandwich)
  library(glue)
})

# -------- Paths --------
FILE_TEXT <- "data/processed/analysis_ready.csv"
FILE_DEALS <- "data/processed/deals_core.xlsx"
DIR_TBL <- "output/tables"
DIR_FIG <- "output/figures"
dir_create(DIR_TBL); dir_create(DIR_FIG)

# -------- Helpers --------
# Robust SE (HC3) + tidy
tidy_robust <- function(mod, vcov_fun = sandwich::vcovHC, type = "HC3"){
  co <- lmtest::coeftest(mod, vcov. = vcov_fun(mod, type = type))
  broom::tidy(co) %>% rename(std.error = std.error)
}

# Standardizza
std <- function(v) as.numeric(scale(v))

# Numerico robusto (gestisce %, virgole, "n.a.")
to_num <- function(x){
  if (is.numeric(x)) return(x)
  x <- gsub(",", "", as.character(x))
  x <- gsub("%", "", x)
  x[x %in% c("n.a.","na","N.A.","N/A","","NULL","NaN","nan","-")] <- NA
  suppressWarnings(as.numeric(x))
}

# Winsorize (per stabilizzare outlier)
winsorize <- function(x, probs=c(0.01,0.99)){
  if (all(is.na(x))) return(x)
  qs <- quantile(x, probs, na.rm=TRUE)
  pmin(pmax(x, qs[1L]), qs[2L])
}

# -------- Load text indices --------
text <- read_csv(FILE_TEXT, show_col_types = FALSE)

# -------- Load deals (mapping esplicito con i tuoi nomi) --------
has_janitor <- requireNamespace("janitor", quietly = TRUE)

# Gestione NA già in lettura
deals_raw <- readxl::read_xlsx(FILE_DEALS, na = c("n.a.","N.A.","N/A","","NA","NULL","-"))
deals <- if (has_janitor) janitor::clean_names(deals_raw) else {
  nm <- names(deals_raw)
  nm <- tolower(gsub("[^a-zA-Z0-9]+","_", nm))
  nm <- make.unique(nm, sep = "_")
  names(deals_raw) <- nm
  deals_raw
}

nm <- names(deals)

# Nomi espliciti (da tua stampa):
#  "target_ticker_symbol", "bid_premium_announced_date_percent",
#  "deal_value_usd" / "deal_value_th_usd", "announced_date"
tick_col <- "target_ticker_symbol"
prem_col <- "bid_premium_announced_date_percent"
ann_col  <- "announced_date"
val_col  <- if ("deal_value_usd" %in% nm) "deal_value_usd" else if ("deal_value_th_usd" %in% nm) "deal_value_th_usd" else NA_character_

message("→ Mappatura colonne:")
message("   ticker  : ", tick_col)
message("   premium : ", prem_col)
message("   value   : ", ifelse(is.na(val_col), "(non trovata)", val_col))
message("   ann_date: ", ann_col)

if (is.na(tick_col) || is.na(prem_col)) stop("Ticker/Premium non trovati nei nomi puliti.")

deals <- deals %>%
  mutate(
    ticker = toupper(trimws(as.character(.data[[tick_col]]))),
    premium_pct = to_num(.data[[prem_col]]),                       # atteso in percentuale (es. 35)
    deal_value  = if (!is.na(val_col)) to_num(.data[[val_col]]) else NA_real_,
    deal_value  = if (!is.na(val_col) && val_col == "deal_value_th_usd") deal_value * 1e3 else deal_value,
    announced_date = suppressWarnings(as.Date(.data[[ann_col]]))
  ) %>%
  filter(!is.na(ticker), !is.na(premium_pct)) %>%
  distinct(ticker, .keep_all = TRUE)

# Se premium è in decimali, converti a percentuale
if (max(deals$premium_pct, na.rm=TRUE) <= 1.5) {
  message("↪ premium appare in decimali: converto a percento (×100).")
  deals$premium_pct <- deals$premium_pct * 100
}

# -------- Merge text indices ↔ deals (by ticker) --------
text2 <- text %>%
  mutate(ticker = toupper(trimws(as.character(ticker)))) %>%
  select(ticker, n_docs, total_words, tone_mean, forward_mean,
         uncertainty_mean, numeric_mean, share_10k, share_10q, share_6k)

dat <- deals %>% inner_join(text2, by = "ticker")

if (nrow(dat) == 0) stop("Merge vuoto: i ticker non matchano tra deals_core.xlsx e analysis_ready.csv.")

message(glue("Merged rows: {nrow(dat)} | tickers deals: {n_distinct(deals$ticker)} | tickers merged: {n_distinct(dat$ticker)}"))
message(glue("Mean premium (%): {round(mean(dat$premium_pct, na.rm=TRUE),1)}"))

# -------- Feature engineering --------
if (!all(is.na(dat$deal_value))) {
  med_val <- median(dat$deal_value, na.rm = TRUE)
  dat$log_val <- log(pmax(replace_na(dat$deal_value, med_val), 1))
} else {
  dat$log_val <- 0
}

dat <- dat %>%
  mutate(
    z_tone    = std(tone_mean),
    z_forward = std(forward_mean),
    z_uncert  = std(uncertainty_mean),
    z_numeric = std(numeric_mean),
    premium_w = winsorize(premium_pct)   # DV winsorizzata (1%–99%)
  )

# -------- Modello OLS (HC3) --------
f_ols <- premium_w ~ z_tone + z_forward + z_uncert + z_numeric + log_val + share_10k + share_10q
mod_ols <- lm(f_ols, data = dat)
tab_ols <- tidy_robust(mod_ols, type = "HC3") %>%
  mutate(
    term = recode(term,
                  "(Intercept)"="Intercept",
                  "z_tone"="Tone (z)",
                  "z_forward"="Forward-looking (z)",
                  "z_uncert"="Uncertainty (z)",
                  "z_numeric"="Numeric specificity (z)",
                  "log_val"="Log(Deal value)",
                  "share_10k"="Share 10-K",
                  "share_10q"="Share 10-Q")
  )

# -------- Salva tabella HTML --------
html_path <- file.path(DIR_TBL, "main_results.html")
html_out <- paste0(
  "<html><head><meta charset='utf-8'><title>Mini OLS Results</title>",
  "<style>body{font-family:Arial,Helvetica,sans-serif;margin:24px;} table{border-collapse:collapse;} th,td{border:1px solid #ddd;padding:8px;} th{background:#f4f4f4;}</style>",
  "</head><body><h2>OLS: Premium (%) — DV winsorizzata, SE HC3</h2>",
  "<p>Specifica: premium_w ~ z_tone + z_forward + z_uncert + z_numeric + log_val + share_10k + share_10q</p>",
  "<table><tr><th>Term</th><th>Estimate</th><th>Std.Error (HC3)</th><th>t</th><th>p</th></tr>",
  paste0(
    sprintf("<tr><td>%s</td><td>%.3f</td><td>%.3f</td><td>%.2f</td><td>%.3f</td></tr>",
            tab_ols$term, tab_ols$estimate, tab_ols$std.error, tab_ols$statistic, tab_ols$p.value),
    collapse = ""
  ),
  sprintf("</table><p>N = %d, R² = %.3f</p></body></html>", nobs(mod_ols), summary(mod_ols)$r.squared)
)
writeLines(html_out, html_path)
message("✅ Saved table: ", html_path)

# -------- Figura: forest dei coefficienti (fix deprecations) --------
coef_plot <- tab_ols %>%
  dplyr::filter(term != "Intercept") %>%
  dplyr::mutate(term = factor(term, levels = rev(term)))

p <- ggplot(coef_plot, aes(y = term, x = estimate)) +
  geom_point() +
  geom_errorbar(aes(xmin = estimate - 1.96*std.error,
                    xmax = estimate + 1.96*std.error),
                width = 0.2) +
  geom_vline(xintercept = 0, linetype = "dashed") +
  labs(x = "Coefficient (robust 95% CI)", y = NULL,
       title = "OLS on Premium (%) — HC3 & Winsorized DV") +
  theme_minimal(base_size = 12)

fig_path <- file.path(DIR_FIG, "coef_ols.png")
ggsave(fig_path, p, width = 7, height = 4.5, dpi = 300)
message("✅ Saved figure: ", fig_path)

# -------- Influence diagnostics (export) --------
lev  <- hatvalues(mod_ols)
cook <- cooks.distance(mod_ols)
diag_df <- dplyr::tibble(
  ticker   = dat$ticker,
  leverage = as.numeric(lev),
  cooks_d  = as.numeric(cook)
) %>% dplyr::arrange(desc(leverage))
readr::write_csv(diag_df, file.path(DIR_TBL, "influence_diagnostics.csv"))
message("✅ Saved diagnostics: ", file.path(DIR_TBL, "influence_diagnostics.csv"))

# -------- Console summary --------
message("=== OLS Summary (HC3) ===")
print(tab_ols)

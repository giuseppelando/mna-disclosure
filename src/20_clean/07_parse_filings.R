# -----------------------------
# parse_filings.R
# Pulisce HTML dei filing SEC e costruisce il corpus testuale pre-annuncio
# Output: data/interim/filings_text.csv
# -----------------------------

suppressPackageStartupMessages({
  library(rvest)
  library(xml2)
  library(stringr)
  library(dplyr)
  library(purrr)
  library(readr)
  library(tidyr)
  library(fs)
  library(glue)
})

# --------- Config ----------
DIR_IN  <- "data/raw/filings"
DIR_OUT <- "data/interim"
OUTFILE <- file.path(DIR_OUT, "filings_text.csv")

MIN_CHARS <- 2000L    # scarta file troppo piccoli
KEEP_FORMS <- c("10-K","10-Q","20-F","6-K")

dir_create(DIR_OUT)

# --------- Helpers ----------
# 1) Parsifica HTML e rimuove rumore mantenendo il testo leggibile
read_and_clean_html <- function(path) {
  # Leggi HTML robusto
  doc <- read_html(path, options = c("RECOVER", "NOERROR", "NOWARNING"))
  # Rimuovi tag rumorosi
  xml_find_all(doc, ".//script|.//style|.//noscript|.//header|.//footer") %>% xml_remove()
  # Togli tabelle di indice/Exhibit molto verbose
  xml_find_all(doc, ".//*[contains(translate(normalize-space(.),'exhibit index','EXHIBIT INDEX'),'EXHIBIT INDEX')]") %>% xml_remove()
  # Testo lineare
  txt <- doc %>% html_text2()
  # Normalizzazioni
  txt <- txt %>%
    str_replace_all("\\r\\n|\\r", "\n") %>%
    str_replace_all("[\u00A0\\t]+", " ") %>%               # NBSP/tab -> spazio
    str_replace_all(" {2,}", " ") %>%
    str_replace_all("\n{3,}", "\n\n") %>%
    str_trim()
  txt
}

# 2) Trova sottosezione tra start_regex e primo match di uno degli end_regex
extract_section <- function(text, start_regex, end_regex_vec, .ignore_case = TRUE) {
  flags <- if (.ignore_case) "(?is)" else "(?s)"
  start <- str_locate(text, regex(paste0(flags, start_regex)))[1,]
  if (is.na(start[1])) return(NA_character_)
  # cerca il primo end successivo
  ends_pos <- map(end_regex_vec, ~ str_locate_all(text, regex(paste0(flags, .x)))[[1]]) %>%
    keep(~ nrow(.x) > 0) %>%
    map(~ .x[,1]) %>%
    unlist(use.names = FALSE)
  ends_pos <- ends_pos[ends_pos > start[2]]
  end_pos <- if (length(ends_pos)) min(ends_pos) - 1L else nchar(text)
  str_sub(text, start[1], end_pos) %>% str_trim()
}

# 3) Heuristics per heading SEC (gestisce apostrofi tipografici)
re_apost <- "[’'`]"
# 10-K MD&A (Item 7), 10-Q MD&A (Item 2)
MDNA_10K_START <- paste0("Item\\s*7\\.?\\s*Management", re_apost, "?s\\s*Discussion\\s*and\\s*Analysis")
MDNA_10Q_START <- paste0("Item\\s*2\\.?\\s*Management", re_apost, "?s\\s*Discussion\\s*and\\s*Analysis")
MDNA_ENDS      <- c("Item\\s*7A\\.", "Item\\s*8\\.", "Quantitative\\s+and\\s+Qualitative\\s+Disclosures\\s+About\\s+Market\\s+Risk")

# Risk Factors (Item 1A)
RISK_START_10K <- "Item\\s*1A\\.?\\s*Risk\\s*Factors"
RISK_ENDS_10K  <- c("Item\\s*1B\\.", "Item\\s*2\\.")
RISK_START_10Q <- "Item\\s*1A\\.?\\s*Risk\\s*Factors"
RISK_ENDS_10Q  <- c("Item\\s*2\\.", "Item\\s*3\\.")

# Outlook/Guidance (best-effort su heading comuni)
OUTLOOK_START  <- "(Outlook|Guidance|Prospects|Future\\s+Outlook|Business\\s+Outlook)\\b"
OUTLOOK_ENDS   <- c("Item\\s*\\d+\\.", "Risk\\s*Factors", "Liquidity\\s+and\\s+Capital\\s+Resources")

# 4) Metriche semplici
lm_positive <- c("achieve","advantage","benefit","growth","improve","strong","opportunity")
lm_negative <- c("adverse","decline","decrease","deteriorat","loss","risk","weak","uncertain","volatility")

count_tokens <- function(text, dict) {
  if (is.na(text) || !nzchar(text)) return(0L)
  str_count(tolower(text), paste0("\\b(", paste0(dict, collapse="|"), ")\\w*\\b"))
}
n_words <- function(text) {
  if (is.na(text) || !nzchar(text)) return(0L)
  str_count(text, "\\b\\w+\\b")
}
tone_lm <- function(text) {
  n <- n_words(text); if (n == 0) return(NA_real_)
  (count_tokens(text, lm_positive) - count_tokens(text, lm_negative)) / n
}
density <- function(text, pattern) {
  n <- n_words(text); if (n == 0) return(NA_real_)
  str_count(tolower(text), pattern) / n
}

FORWARD_WORDS <- c("will","would","expect","expects","expected","forecast","forecasts","project","projects",
                   "plan","plans","target","targets","anticipate","anticipates","intend","intends","guidance")

UNCERTAINTY_WORDS <- c("uncertain","uncertainty","unknown","unpredictable","indeterminate")

numeric_density <- function(text) density(text, "\\b\\d+(?:[\\.,]\\d+)?%?\\b")
forward_density <- function(text) density(text, paste0("\\b(", paste(FORWARD_WORDS, collapse="|"), ")\\b"))
uncert_density  <- function(text) density(text, paste0("\\b(", paste(UNCERTAINTY_WORDS, collapse="|"), ")\\b"))

# 5) Parser del filename: <TICKER>_<FORM>_<YYYY-MM-DD>.html  (es: AHL_10-Q_2018-08-08.html)
parse_filename_meta <- function(path) {
  fname <- path_file(path)
  m <- str_match(fname, "^([A-Za-z0-9\\-\\.]+)_((?:10\\-K|10\\-Q|20\\-F|6\\-K))_((?:19|20)\\d{2}\\-\\d{2}\\-\\d{2})\\.html$")
  tibble(
    file = path,
    ticker = m[,2],
    form = m[,3],
    filing_date = as.Date(m[,4])
  )
}

# --------- Core pipeline ----------
parse_one_file <- function(path) {
  meta <- parse_filename_meta(path)
  if (is.na(meta$form)) {
    return(tibble(file = path, parse_ok = FALSE, reason = "filename_not_parsed"))
  }
  if (!(meta$form %in% KEEP_FORMS)) {
    return(tibble(file = path, parse_ok = FALSE, reason = "form_not_in_scope"))
  }
  
  txt <- tryCatch(read_and_clean_html(path), error = function(e) NA_character_)
  if (is.na(txt) || nchar(txt) < MIN_CHARS) {
    return(meta %>% mutate(parse_ok = FALSE, reason = "too_short_or_read_error"))
  }
  
  # Sezioni: scegli regex in base al form
  mdna <- if (meta$form %in% c("10-K","20-F")) {
    extract_section(txt, MDNA_10K_START, MDNA_ENDS)
  } else {
    extract_section(txt, MDNA_10Q_START, MDNA_ENDS)
  }
  risk <- if (meta$form %in% c("10-K","20-F")) {
    extract_section(txt, RISK_START_10K, RISK_ENDS_10K)
  } else {
    extract_section(txt, RISK_START_10Q, RISK_ENDS_10Q)
  }
  outlook <- extract_section(txt, OUTLOOK_START, OUTLOOK_ENDS)
  
  # Metriche su testo complessivo "sezione-centrico": se MD&A esiste usala; altrimenti fallback al testo intero
  base_text <- if (!is.na(mdna) && nzchar(mdna)) mdna else txt
  
  tibble(
    file = meta$file,
    ticker = meta$ticker,
    form = meta$form,
    filing_date = meta$filing_date,
    n_chars = nchar(txt),
    n_words = n_words(txt),
    has_mdna = !is.na(mdna) && nzchar(mdna),
    has_risk = !is.na(risk) && nzchar(risk),
    has_outlook = !is.na(outlook) && nzchar(outlook),
    text_clean = txt,
    text_mdna = mdna,
    text_risk = risk,
    text_outlook = outlook,
    tone_lm = tone_lm(base_text),
    forward_density = forward_density(base_text),
    uncertainty_density = uncert_density(base_text),
    numeric_specificity = numeric_density(base_text),
    parse_ok = TRUE,
    reason = NA_character_
  )
}

run_pipeline <- function(dir_in = DIR_IN) {
  files <- dir_ls(dir_in, recurse = TRUE, glob = "*.html")
  if (length(files) == 0) {
    message("Nessun file .html trovato in ", dir_in)
    return(invisible(NULL))
  }
  message("Trovati ", length(files), " file. Avvio parsing…")
  
  res <- map_dfr(files, parse_one_file)
  
  # Filtra i soli parse_ok e sopra soglia minima
  out <- res %>%
    filter(parse_ok, n_chars >= MIN_CHARS) %>%
    # ordina per filing_date
    arrange(filing_date, ticker, form)
  
  write_csv(out, OUTFILE)
  message("Scritto: ", OUTFILE, " (", nrow(out), " righe)")
  
  # piccolo report qualità
  qc <- list(
    total_files = length(files),
    parsed_ok = sum(res$parse_ok, na.rm = TRUE),
    kept_rows = nrow(out),
    share_mdna = mean(out$has_mdna, na.rm = TRUE),
    share_risk = mean(out$has_risk, na.rm = TRUE),
    mean_forward = mean(out$forward_density, na.rm = TRUE),
    mean_tone = mean(out$tone_lm, na.rm = TRUE)
  )
  print(qc)
  invisible(out)
}

if (sys.nframe() == 0) {
  run_pipeline()
}

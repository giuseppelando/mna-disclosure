# src/10_ingest/get_filings.R
suppressPackageStartupMessages({
  library(dplyr); library(lubridate); library(readr); library(stringr)
  library(httr); library(jsonlite); library(fs); library(glue); library(purrr); library(tidyr)
})

# ==== PARAMS (puoi spostarle in config/params.yml più avanti) ====
ua         <- getOption("edgarWebR.user_agent", "")
stopifnot(nzchar(ua))  # user-agent obbligatoria
days_back  <- 180       # finestra pre-annuncio: [-180, -1]
min_size_b <- 1500      # scarta file troppo piccoli (bytes)
max_docs_per_issuer <- 6

# Form PRE-annuncio (periodiche/earnings dei target)
forms_pre <- c("10-K","10-Q","20-F","6-K")
# 8-K earnings (Item 2.02) lo filtreremo più avanti sul contenuto; per ora lo escludiamo dal pre
include_8k_earnings <- FALSE  # metti TRUE se vuoi includere 8-K earnings (poi serve filtro contenuto)

# ==== INPUT ====
stopifnot(file.exists("data/interim/deals_with_cik.csv"))
base <- readr::read_csv("data/interim/deals_with_cik.csv", show_col_types = FALSE)

# Riconosci automaticamente il nome della colonna di annuncio
announce_col <- dplyr::case_when(
  "announce_date"   %in% names(base) ~ "announce_date",
  "announced_date"  %in% names(base) ~ "announced_date",
  TRUE ~ NA_character_
)
if (is.na(announce_col)) {
  stop("Manca la data di annuncio: aggiungi una colonna 'announce_date' oppure 'announced_date' nel CSV.")
}

# Standardizza: crea sempre 'announce_date' come Date
base <- base %>%
  dplyr::mutate(announce_date = as.Date(.data[[announce_col]]))

deals <- base %>%
  dplyr::filter(!is.na(cik)) %>%
  dplyr::mutate(
    cik_pad = as.character(cik),
    cik_int = suppressWarnings(as.integer(cik_pad))
  ) %>%
  dplyr::filter(!is.na(cik_int))


# ==== OUTPUT ====
fs::dir_create("data/raw/filings")
fs::dir_create("data/interim/submissions_cache")
log_path <- "data/interim/get_filings_log.csv"
logs <- list()

# ==== HELPERS ====
ua_hdr <- httr::add_headers(`User-Agent` = ua)

cache_file <- function(cik_pad) file.path("data/interim/submissions_cache", sprintf("CIK%s.json", cik_pad))

fetch_submissions <- function(cik_pad) {
  cf <- cache_file(cik_pad)
  if (file.exists(cf)) {
    txt <- readr::read_file(cf)
  } else {
    url <- sprintf("https://data.sec.gov/submissions/CIK%s.json", cik_pad)
    resp <- httr::GET(url, ua_hdr)
    if (httr::http_error(resp)) return(NULL)
    txt <- httr::content(resp, "text", encoding = "UTF-8")
    writeLines(txt, cf, useBytes = TRUE)
  }
  jsonlite::fromJSON(txt)
}

doc_url <- function(cik_int, acc_no, primary_doc) {
  acc_clean <- gsub("-", "", acc_no)
  sprintf("https://www.sec.gov/Archives/edgar/data/%s/%s/%s", cik_int, acc_clean, primary_doc)
}

within_window <- function(filing_date, announce_date, days_back) {
  !is.na(announce_date) && !is.na(filing_date) &&
    filing_date >= (announce_date - days_back) && filing_date <= (announce_date - 1)
}

# ==== MAIN LOOP ====
n_tot <- nrow(deals)
logs <- list()

for (i in seq_len(n_tot)) {
  row <- deals[i,]
  
  # inizializza contatori/flag SEMPRE
  found_n <- 0L
  kept    <- 0L
  reason  <- NA_character_
  
  # 0) announce_date deve esserci per questo deal
  ann <- as.Date(row$announce_date)
  if (is.na(ann)) {
    reason <- "no_announce_date"
    logs[[length(logs)+1]] <- tibble(
      deal_number   = row$deal_number,
      target_ticker = row$target_ticker,
      cik           = row$cik,
      found = found_n, kept = kept, reason = reason
    )
    message(glue("[{i}/{n_tot}] {row$target_ticker} {row$cik}: skipped (no announce_date)"))
    next
  }
  
  # 1) submissions JSON (con caching)
  Sys.sleep(0.5)
  subm <- fetch_submissions(row$cik_pad)
  if (is.null(subm) || is.null(subm$filings$recent)) {
    reason <- "no_submissions_json"
    logs[[length(logs)+1]] <- tibble(
      deal_number   = row$deal_number,
      target_ticker = row$target_ticker,
      cik           = row$cik,
      found = found_n, kept = kept, reason = reason
    )
    message(glue("[{i}/{n_tot}] {row$target_ticker} {row$cik}: no submissions"))
    next
  }
  
  # 2) build tabella filings recent
  recent <- subm$filings$recent
  df <- tibble(
    form        = recent$form,
    filing_date = as.Date(recent$filingDate),
    accession   = recent$accessionNumber,
    primary     = recent$primaryDocument
  ) |>
    mutate(cik_int = row$cik_int) |>
    distinct(accession, .keep_all = TRUE)
  
  # 3) Seleziona form pre-annuncio e finestra [-days_back,-1]
  df <- df |>
    filter(form %in% forms_pre) |>
    filter(!is.na(filing_date),
           filing_date >= (ann - days_back),
           filing_date <= (ann - 1))
  
  found_n <- nrow(df)
  if (found_n == 0L) {
    reason <- "no_docs_in_window_pre"
    logs[[length(logs)+1]] <- tibble(
      deal_number   = row$deal_number,
      target_ticker = row$target_ticker,
      cik           = row$cik,
      found = found_n, kept = kept, reason = reason
    )
    message(glue("[{i}/{n_tot}] {row$target_ticker} {row$cik}: 0 docs in pre-window"))
    next
  }
  
  # 4) Cap: priorità a 10-K/20-F, poi più recenti
  df <- df |>
    arrange(desc(form %in% c("10-K","20-F")), desc(filing_date)) |>
    slice_head(n = max_docs_per_issuer)
  
  # 5) Download con gate di dimensione
  tdir <- file.path("data/raw/filings", row$target_ticker %||% "NA")
  fs::dir_create(tdir)
  
  for (k in seq_len(nrow(df))) {
    u <- doc_url(df$cik_int[k], df$accession[k], df$primary[k])
    dest <- file.path(
      tdir,
      sprintf("%s_%s_%s.html",
              row$target_ticker %||% "NA",
              df$form[k],
              format(df$filing_date[k], "%Y-%m-%d"))
    ) |>
      str_replace_all("[: ]","-")
    
    Sys.sleep(0.25)
    ok <- tryCatch({
      httr::GET(u, ua_hdr, write_disk(dest, overwrite = TRUE))
      TRUE
    }, error = function(e) FALSE)
    
    if (ok && file.exists(dest)) {
      sz <- file.info(dest)$size
      if (!is.na(sz) && sz >= min_size_b) {
        kept <- kept + 1L
      } else {
        try(unlink(dest), silent = TRUE)
      }
    }
  }
  
  reason <- ifelse(kept > 0L, "ok_pre", "all_too_small_or_failed")
  logs[[length(logs)+1]] <- tibble(
    deal_number   = row$deal_number,
    target_ticker = row$target_ticker,
    cik           = row$cik,
    found = found_n, kept = kept, reason = reason
  )
  message(glue("[{i}/{n_tot}] {row$target_ticker} {row$cik}: found {found_n}, kept {kept}"))
}

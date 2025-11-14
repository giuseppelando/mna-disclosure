# src/10_ingest/get_filings.R
suppressPackageStartupMessages({
  library(dplyr); library(lubridate); library(readr); library(stringr)
  library(httr); library(jsonlite); library(fs); library(glue); library(purrr)
})

# User-Agent
ua <- getOption("edgarWebR.user_agent", "")
stopifnot(nzchar(ua))

# Input deals con CIK
stopifnot(file.exists("data/interim/deals_with_cik.csv"))
deals <- readr::read_csv("data/interim/deals_with_cik.csv", show_col_types = FALSE) |>
  filter(!is.na(cik))

# Finestra pre-annuncio (proxy su last_status_date); prova 365 giorni
win_days <- 365

# Form utili M&A
form_set <- c("8-K","425","S-4","DEFM14A","PREM14A","10-K","10-Q","6-K","20-F")

# Output
fs::dir_create("data/raw/filings")
log_path <- "data/interim/get_filings_log.csv"
logs <- list()

# Helper: scarica submissions JSON per un CIK (padded)
fetch_submissions <- function(cik_pad) {
  url <- sprintf("https://data.sec.gov/submissions/CIK%s.json", cik_pad)
  resp <- httr::GET(url, httr::add_headers(`User-Agent` = ua))
  if (httr::http_error(resp)) return(NULL)
  jsonlite::fromJSON(httr::content(resp, "text", encoding = "UTF-8"))
}

# Helper: costruisci URL documento da (cik_int, accessionNumber con -, primaryDocument)
doc_url <- function(cik_int, acc_no, primary_doc) {
  # path usa CIK senza zeri iniziali e accession senza trattini
  acc_clean <- gsub("-", "", acc_no)
  sprintf("https://www.sec.gov/Archives/edgar/data/%s/%s/%s", cik_int, acc_clean, primary_doc)
}

n_tot <- nrow(deals)
for (i in seq_len(n_tot)) {
  row <- deals[i,]
  cik_pad <- as.character(row$cik)
  cik_int <- suppressWarnings(as.integer(cik_pad))
  if (is.na(cik_int)) {
    logs[[length(logs)+1]] <- tibble(target_ticker=row$target_ticker, cik=row$cik, n_filings=0, note="bad_cik")
    next
  }
  
  # submissions JSON
  Sys.sleep(0.6) # cortesia
  subm <- fetch_submissions(cik_pad)
  if (is.null(subm) || is.null(subm$filings$recent)) {
    logs[[length(logs)+1]] <- tibble(target_ticker=row$target_ticker, cik=row$cik, n_filings=0, note="no_submissions_json")
    message(glue("[{i}/{n_tot}] {row$target_ticker} {row$cik}: no submissions"))
    next
  }
  
  recent <- subm$filings$recent
  # recent ha colonne parallele (vettrici): form, filingDate, accessionNumber, primaryDocument, ecc.
  df <- tibble(
    form = recent$form,
    filing_date = as.Date(recent$filingDate),
    accession = recent$accessionNumber,
    primary = recent$primaryDocument
  ) |>
    mutate(cik_int = cik_int)
  
  # filtro per form
  df <- df |> filter(form %in% form_set)
  
  # filtro temporale, se abbiamo la data del deal
  if (!is.na(row$last_status_date)) {
    after  <- as.Date(row$last_status_date) - win_days
    before <- as.Date(row$last_status_date) - 1
    df <- df |> filter(!is.na(filing_date), filing_date >= after, filing_date <= before)
  }
  
  # fallback: se è rimasto vuoto, prova senza finestra (solo per non perdere tutto)
  phase <- "win365"
  if (nrow(df) == 0) {
    df <- tibble(
      form = recent$form,
      filing_date = as.Date(recent$filingDate),
      accession = recent$accessionNumber,
      primary = recent$primaryDocument,
      cik_int = cik_int
    ) |>
      filter(form %in% form_set)
    phase <- "nofilter"
  }
  
  n_ok <- 0L
  if (nrow(df) > 0) {
    ticker_dir <- file.path("data/raw/filings", row$target_ticker %||% "NA")
    fs::dir_create(ticker_dir)
    
    for (k in seq_len(nrow(df))) {
      u <- doc_url(df$cik_int[k], df$accession[k], df$primary[k])
      dest <- file.path(ticker_dir,
                        sprintf("%s_%s_%s.html",
                                row$target_ticker %||% "NA",
                                df$form[k],
                                format(df$filing_date[k], "%Y-%m-%d"))) |>
        str_replace_all("[: ]","-")
      
      Sys.sleep(0.3)
      ok <- tryCatch({
        httr::GET(u, httr::add_headers(`User-Agent` = ua), write_disk(dest, overwrite = TRUE))
        TRUE
      }, error = function(e) FALSE)
      
      if (ok) n_ok <- n_ok + 1L
    }
  }
  
  logs[[length(logs)+1]] <- tibble(
    target_ticker = row$target_ticker, cik = row$cik,
    n_filings = n_ok, note = ifelse(n_ok>0, paste0("ok_", phase), paste0("no_docs_", phase))
  )
  message(glue("[{i}/{n_tot}] {row$target_ticker} CIK {row$cik}: scaricati {n_ok} doc (", phase, ")"))
}

if (length(logs) > 0) {
  readr::write_csv(bind_rows(logs), log_path)
  message("Log scritto → ", log_path)
}

message("Download completato.")

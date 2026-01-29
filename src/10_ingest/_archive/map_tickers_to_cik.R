# src/10_ingest/map_tickers_to_cik.R
suppressPackageStartupMessages({
  library(dplyr); library(readr); library(jsonlite); library(httr); library(stringr); library(fs)
})

# 0) Input deals puliti
stopifnot(file.exists("data/interim/deals_clean.csv"))
deals <- readr::read_csv("data/interim/deals_clean.csv", show_col_types = FALSE)

# 1) Prepara cartelle
fs::dir_create("data/interim")

# 2) Scarica il mapping bulk TICKER→CIK dalla SEC (UNA VOLTA)
ua <- "Giuseppe <giuseppe.lando@studbocconi.it> ; mna-disclosure thesis"
sec_url <- "https://www.sec.gov/files/company_tickers.json"
dest_json <- "data/interim/sec_company_tickers.json"

if (!file.exists(dest_json)) {
  resp <- httr::GET(sec_url, httr::add_headers(`User-Agent` = ua))
  stop_for_status(resp)
  writeBin(content(resp, "raw"), dest_json)
}

# 3) Leggi e normalizza
raw <- jsonlite::fromJSON(dest_json, simplifyDataFrame = TRUE)

# Se 'raw' è una lista con indici numerici, convertila in data.frame
if (is.list(raw) && !is.data.frame(raw)) {
  raw <- dplyr::bind_rows(raw)
}

stopifnot(all(c("cik_str", "ticker", "title") %in% names(raw)))

sec_map <- raw |>
  transmute(
    target_ticker = toupper(trimws(ticker)),
    cik = stringr::str_pad(as.character(cik_str), width = 10, side = "left", pad = "0"),
    sec_name = title
  ) |>
  distinct(target_ticker, .keep_all = TRUE)

readr::write_csv(sec_map, "data/interim/ticker_cik.csv")

# 4) Join con i deals
deals_cik <- deals |>
  mutate(target_ticker = toupper(trimws(target_ticker))) |>
  left_join(sec_map, by = "target_ticker")

readr::write_csv(deals_cik, "data/interim/deals_with_cik.csv")

message("Creato mapping TICKER→CIK via bulk SEC. Righe con CIK: ",
        sum(!is.na(deals_cik$cik)), "/", nrow(deals_cik))
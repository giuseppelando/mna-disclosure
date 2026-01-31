# src/20_resolve/21_cik_resolve_v2.R

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(tibble)
  library(glue)
  library(httr)
  library(jsonlite)
  library(xml2)
  library(rvest)
  library(stringdist)
  library(purrr)
})

source("src/utils/sec_http.R")
source("src/utils/sec_cik_lookup.R")

CONFIG <- list(
  deals_in = "data/interim/deals_sample.rds",
  deals_out = "data/interim/deals_with_cik.rds",
  unmatched_out = "data/interim/cik_unmatched_deals.csv",
  target_map_out = "data/interim/target_cik_map.rds",
  request_delay = 0.2,
  min_name_sim_validate = 0.75
)

log_file <- "data/interim/cik_resolve_log.txt"
if (!dir.exists(dirname(log_file))) dir.create(dirname(log_file), recursive = TRUE)

log_msg <- function(msg, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  line <- glue("[{timestamp}] [{level}] {msg}")
  cat(line, "\n")
  cat(line, "\n", file = log_file, append = TRUE)
}

log_msg("======================================================================")
log_msg("STEP 2 (v2): RESOLVE TARGET CIK")
log_msg("======================================================================")
log_msg("CIK resolution process started")

ua <- sec_user_agent()

deals <- readRDS(CONFIG$deals_in)
log_msg(glue("Loaded {nrow(deals)} deals from {CONFIG$deals_in}"))

# Ensure expected columns exist (no-op if already there)
if (!("deal_id" %in% names(deals))) deals$deal_id <- seq_len(nrow(deals))
if (!("target_cik" %in% names(deals))) deals$target_cik <- NA_character_
if (!("cik_source" %in% names(deals))) deals$cik_source <- NA_character_
if (!("cik_confidence" %in% names(deals))) deals$cik_confidence <- NA_character_

# Build entity keys (resolve at target-entity level to avoid repeated calls)
deals_keyed <- deals %>%
  mutate(
    target_ticker_lookup = clean_ticker_for_lookup(target_ticker),
    target_name_clean = clean_name_basic(target_name),
    entity_key = if_else(!is.na(target_ticker_lookup) & target_ticker_lookup != "",
                         paste0("T:", target_ticker_lookup),
                         paste0("N:", target_name_clean))
  )

targets <- deals_keyed %>%
  select(entity_key, target_name, target_name_clean, target_ticker, target_ticker_lookup) %>%
  distinct()

targets <- targets %>%
  mutate(
    target_cik = NA_character_,
    cik_source = NA_character_,
    cik_confidence = NA_character_,
    sec_company_name = NA_character_
  )

# =========================
# STRATEGY 1: SEC bulk ticker file (current associations)
# =========================
log_msg("")
log_msg("STRATEGY 1: SEC Bulk Ticker File")
log_msg(strrep("-", 50))

bulk_url <- "https://www.sec.gov/files/company_tickers.json"
bulk_resp <- try(sec_get(bulk_url, ua = ua, delay = CONFIG$request_delay), silent = TRUE)
bulk_tickers <- NULL

if (!inherits(bulk_resp, "try-error")) {
  bulk_txt <- httr::content(bulk_resp, as = "text", encoding = "UTF-8")
  bulk_json <- jsonlite::fromJSON(bulk_txt)
  bulk_tickers <- tibble::tibble(
    cik = bulk_json$cik_str,
    ticker = bulk_json$ticker,
    name = bulk_json$title
  ) %>%
    mutate(
      cik10 = format_cik_10(cik),
      ticker_lookup = clean_ticker_for_lookup(ticker)
    ) %>%
    filter(!is.na(cik10), !is.na(ticker_lookup), ticker_lookup != "") %>%
    distinct(ticker_lookup, .keep_all = TRUE)
  
  log_msg(glue("Downloaded {nrow(bulk_tickers)} companies"))
  
  before <- sum(!is.na(targets$target_cik))
  targets <- targets %>%
    left_join(bulk_tickers %>% select(ticker_lookup, cik10, name),
              by = c("target_ticker_lookup" = "ticker_lookup")) %>%
    mutate(
      target_cik = coalesce(target_cik, cik10),
      cik_source = if_else(!is.na(cik10) & is.na(cik_source), "bulk_ticker", cik_source),
      cik_confidence = if_else(!is.na(cik10) & is.na(cik_confidence), "high", cik_confidence),
      sec_company_name = if_else(!is.na(cik10) & is.na(sec_company_name), name, sec_company_name)
    ) %>%
    select(-cik10, -name)
  
  matched <- sum(!is.na(targets$target_cik)) - before
  log_msg(glue("Matched {matched} targets via bulk ticker file"))
} else {
  log_msg("Failed to download SEC bulk ticker file", "WARN")
}

# =========================
# STRATEGY 2: Browse-EDGAR lookup by ticker (covers many non-current tickers)
# =========================
log_msg("")
log_msg("STRATEGY 2: Browse-EDGAR lookup (Ticker)")
log_msg(strrep("-", 50))

need2 <- targets %>%
  filter(is.na(target_cik), !is.na(target_ticker_lookup), target_ticker_lookup != "")

log_msg(glue("Unmatched targets with ticker: {nrow(need2)}"))

if (nrow(need2) > 0) {
  for (i in seq_len(nrow(need2))) {
    row <- need2[i, ]
    
    # try both with and without dash (class shares)
    q_terms <- unique(na.omit(c(
      row$target_ticker_lookup,
      gsub("-", "", row$target_ticker_lookup)
    )))
    
    got <- FALSE
    for (term in q_terms) {
      cands <- browse_edgar_lookup(term, ua = ua, delay = CONFIG$request_delay)
      if (nrow(cands) == 0) next
      
      best <- pick_best_cik(
        candidates = cands,
        target_name_clean = row$target_name_clean,
        target_ticker_clean = row$target_ticker_lookup,
        ua = ua,
        delay = CONFIG$request_delay
      )
      
      if (!is.na(best$cik) && best$confidence != "reject") {
        targets <- targets %>%
          mutate(
            target_cik = if_else(entity_key == row$entity_key, best$cik, target_cik),
            cik_source = if_else(entity_key == row$entity_key, "browse_ticker", cik_source),
            cik_confidence = if_else(entity_key == row$entity_key, best$confidence, cik_confidence),
            sec_company_name = if_else(entity_key == row$entity_key, best$sec_company_name, sec_company_name)
          )
        got <- TRUE
        break
      }
    }
    
    if (i %% 200 == 0) log_msg(glue("  Processed {i}/{nrow(need2)} ticker targets..."), "DEBUG")
  }
}

log_msg(glue("Matched targets so far: {sum(!is.na(targets$target_cik))} / {nrow(targets)}"))

# =========================
# STRATEGY 3: Browse-EDGAR lookup by name (post-cleaning)
# =========================
log_msg("")
log_msg("STRATEGY 3: Browse-EDGAR lookup (Company Name)")
log_msg(strrep("-", 50))

need3 <- targets %>% filter(is.na(target_cik), target_name_clean != "")

log_msg(glue("Unmatched targets with name: {nrow(need3)}"))

if (nrow(need3) > 0) {
  for (i in seq_len(nrow(need3))) {
    row <- need3[i, ]
    
    # Use a shorter query if the cleaned name is very long
    toks <- unlist(strsplit(row$target_name_clean, " "))
    q_name <- paste(head(toks, 6), collapse = " ")
    if (nchar(q_name) < 4) next
    
    cands <- browse_edgar_lookup(q_name, ua = ua, delay = CONFIG$request_delay)
    if (nrow(cands) > 0) {
      best <- pick_best_cik(
        candidates = cands,
        target_name_clean = row$target_name_clean,
        target_ticker_clean = row$target_ticker_lookup,
        ua = ua,
        delay = CONFIG$request_delay
      )
      
      if (!is.na(best$cik) && best$confidence != "reject") {
        targets <- targets %>%
          mutate(
            target_cik = if_else(entity_key == row$entity_key, best$cik, target_cik),
            cik_source = if_else(entity_key == row$entity_key, "browse_name", cik_source),
            cik_confidence = if_else(entity_key == row$entity_key, best$confidence, cik_confidence),
            sec_company_name = if_else(entity_key == row$entity_key, best$sec_company_name, sec_company_name)
          )
      }
    }
    
    if (i %% 200 == 0) log_msg(glue("  Processed {i}/{nrow(need3)} name targets..."), "DEBUG")
  }
}

# =========================
# Write target map + join back to deals
# =========================
saveRDS(targets, CONFIG$target_map_out)
log_msg(glue("Saved target map: {CONFIG$target_map_out}"))

deals_out <- deals_keyed %>%
  left_join(
    targets %>% select(entity_key, target_cik, cik_source, cik_confidence, sec_company_name),
    by = "entity_key"
  ) %>%
  mutate(
    target_cik = coalesce(target_cik.x, target_cik.y),
    cik_source = coalesce(cik_source.x, cik_source.y),
    cik_confidence = coalesce(cik_confidence.x, cik_confidence.y)
  ) %>%
  select(-target_cik.x, -target_cik.y, -cik_source.x, -cik_source.y, -cik_confidence.x, -cik_confidence.y) %>%
  select(-target_ticker_lookup, -target_name_clean, -entity_key)

# Summary
total <- nrow(deals_out)
with_cik <- sum(!is.na(deals_out$target_cik))
without_cik <- total - with_cik

log_msg("")
log_msg(strrep("=", 70))
log_msg("CIK RESOLUTION SUMMARY")
log_msg(strrep("=", 70))
log_msg(glue("Total deals: {total}"))
log_msg(glue("With CIK: {with_cik} ({round(100*with_cik/total, 1)}%)"))
log_msg(glue("Without CIK: {without_cik} ({round(100*without_cik/total, 1)}%)"))

src_tab <- deals_out %>%
  filter(!is.na(target_cik)) %>%
  count(cik_source, sort = TRUE)

if (nrow(src_tab) > 0) {
  log_msg("")
  log_msg("Match sources:")
  for (i in seq_len(nrow(src_tab))) {
    log_msg(glue("  {src_tab$cik_source[i]}: {src_tab$n[i]}"))
  }
}

conf_tab <- deals_out %>%
  filter(!is.na(target_cik)) %>%
  count(cik_confidence, sort = TRUE)

if (nrow(conf_tab) > 0) {
  log_msg("")
  log_msg("Confidence levels:")
  for (i in seq_len(nrow(conf_tab))) {
    log_msg(glue("  {conf_tab$cik_confidence[i]}: {conf_tab$n[i]}"))
  }
}

# Save outputs
saveRDS(deals_out, CONFIG$deals_out)
log_msg(glue("Saved: {CONFIG$deals_out}"))

unmatched <- deals_out %>% filter(is.na(target_cik))
readr::write_csv(unmatched, CONFIG$unmatched_out)
log_msg(glue("Saved: {CONFIG$unmatched_out} ({nrow(unmatched)} deals)"))

log_msg("CIK resolution complete")
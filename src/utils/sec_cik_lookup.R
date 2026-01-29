# src/utils/sec_cik_lookup.R

# Requires: httr, rvest, xml2, dplyr, stringr, tibble, purrr
# Depends on: src/utils/sec_http.R

format_cik_10 <- function(x) {
  if (is.na(x) || x == "") return(NA_character_)
  x <- gsub("\\D+", "", as.character(x))
  if (nchar(x) == 0) return(NA_character_)
  sprintf("%010s", x)
}

# Ticker cleaning for SEC lookup (handles Refinitiv-like suffixes e.g. IBM.N, ABC:US)
clean_ticker_for_lookup <- function(t) {
  t <- toupper(trimws(as.character(t)))
  if (is.na(t) || t == "") return(NA_character_)
  t <- gsub("\\s+", "", t)
  # strip common separators/suffixes (keep left side)
  t <- sub("[:].*$", "", t)
  t <- sub("[.].*$", "", t)
  # keep letters/numbers and dash (class shares)
  t <- gsub("[^A-Z0-9-]", "", t)
  if (t == "") NA_character_ else t
}

# Company name normalization for matching/scoring
clean_name_basic <- function(x) {
  x <- toupper(trimws(as.character(x)))
  if (is.na(x) || x == "") return("")
  x <- gsub("\\(.*?\\)", " ", x)
  x <- gsub("[^A-Z0-9 ]", " ", x)
  x <- gsub("\\s+", " ", x)
  x <- trimws(x)
  # drop very common legal suffix tokens (light-touch)
  drop <- c("INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY",
            "LTD", "LIMITED", "PLC", "LLC", "LP", "HOLDINGS", "GROUP")
  toks <- unlist(strsplit(x, " "))
  toks <- toks[!(toks %in% drop)]
  x2 <- paste(toks, collapse = " ")
  trimws(x2)
}

name_similarity_jw <- function(a, b) {
  # Jaro-Winkler similarity in [0,1]
  if (a == "" || b == "") return(0)
  1 - stringdist::stringdist(a, b, method = "jw", p = 0.1)
}

# ---- Browse-EDGAR lookup (ticker or name) ----

browse_edgar_lookup <- function(term,
                                ua,
                                delay = 0.2,
                                cache_dir = "data/interim/sec_cache/browse_edgar") {
  
  term <- trimws(as.character(term))
  if (is.na(term) || term == "") {
    return(tibble::tibble(cik = character(), sec_name = character()))
  }
  
  cache_file <- cache_path_for_key(cache_dir, paste0("browse_", term))
  cached <- cache_read(cache_file)
  if (!is.null(cached)) return(cached)
  
  base_url <- "https://www.sec.gov/cgi-bin/browse-edgar"
  q <- list(action = "getcompany", CIK = term, owner = "exclude", count = "100")
  
  # 1) Try ATOM (sometimes returns a single resolved entity)
  out <- try({
    resp <- sec_get(base_url, query = c(q, list(output = "atom")), ua = ua, delay = delay)
    raw <- httr::content(resp, as = "raw")
    txt <- rawToChar(raw)
    doc <- xml2::read_xml(txt)
    
    cik_node  <- xml2::xml_find_first(doc, ".//*[local-name()='company-info']/*[local-name()='cik']")
    name_node <- xml2::xml_find_first(doc, ".//*[local-name()='company-info']/*[local-name()='conformed-name']")
    
    cik  <- if (!is.na(cik_node))  xml2::xml_text(cik_node)  else NA_character_
    name <- if (!is.na(name_node)) xml2::xml_text(name_node) else NA_character_
    
    res <- tibble::tibble(
      cik = format_cik_10(cik),
      sec_name = as.character(name)
    ) %>% dplyr::filter(!is.na(cik))
    
    if (nrow(res) > 0) res else NULL
  }, silent = TRUE)
  
  if (inherits(out, "try-error")) out <- NULL
  
  # 2) Fallback: HTML parse (works for multi-hit name searches)
  if (is.null(out)) {
    resp <- try(sec_get(base_url, query = q, ua = ua, delay = delay), silent = TRUE)
    if (!inherits(resp, "try-error")) {
      html <- httr::content(resp, as = "text", encoding = "UTF-8")
      page <- rvest::read_html(html)
      
      links <- rvest::html_elements(page, "a")
      href  <- rvest::html_attr(links, "href")
      text  <- rvest::html_text2(links)
      
      keep <- !is.na(href) & grepl("browse-edgar\\?", href) & grepl("CIK=", href)
      href <- href[keep]
      text <- text[keep]
      
      # extract cik from href
      cik <- stringr::str_match(href, "CIK=([0-9]{1,10})")[,2]
      out <- tibble::tibble(
        cik = format_cik_10(cik),
        sec_name = text
      ) %>%
        dplyr::filter(!is.na(cik)) %>%
        dplyr::distinct(cik, .keep_all = TRUE)
    }
  }
  
  if (is.null(out)) out <- tibble::tibble(cik = character(), sec_name = character())
  
  cache_write(cache_file, out)
  out
}

# ---- Validate candidate CIK via submissions JSON ----

sec_submissions_get <- function(cik10,
                                ua,
                                delay = 0.2,
                                cache_dir = "data/interim/sec_cache/submissions") {
  if (is.na(cik10) || cik10 == "") return(NULL)
  
  cache_file <- cache_path_for_key(cache_dir, paste0("sub_", cik10))
  cached <- cache_read(cache_file)
  if (!is.null(cached)) return(cached)
  
  url <- paste0("https://data.sec.gov/submissions/CIK", cik10, ".json")
  resp <- try(sec_get(url, query = list(), ua = ua, delay = delay), silent = TRUE)
  if (inherits(resp, "try-error")) return(NULL)
  
  txt <- httr::content(resp, as = "text", encoding = "UTF-8")
  obj <- try(jsonlite::fromJSON(txt, simplifyVector = TRUE), silent = TRUE)
  if (inherits(obj, "try-error")) return(NULL)
  
  cache_write(cache_file, obj)
  obj
}

score_candidate <- function(target_name_clean,
                            target_ticker_clean,
                            cik10,
                            ua,
                            delay = 0.2,
                            min_name_sim = 0.75) {
  
  sub <- sec_submissions_get(cik10, ua = ua, delay = delay)
  if (is.null(sub)) {
    return(list(ok = FALSE, confidence = "reject", sec_company_name = NA_character_))
  }
  
  sec_company <- if (!is.null(sub$companyName)) sub$companyName else NA_character_
  sec_name_clean <- clean_name_basic(sec_company)
  
  sim <- name_similarity_jw(target_name_clean, sec_name_clean)
  
  sec_tickers <- character()
  if (!is.null(sub$tickers)) sec_tickers <- toupper(sub$tickers)
  
  tmatch <- (!is.na(target_ticker_clean) && target_ticker_clean != "" && target_ticker_clean %in% sec_tickers)
  
  ok <- tmatch || (sim >= min_name_sim)
  
  conf <- dplyr::case_when(
    !ok ~ "reject",
    tmatch ~ "high",
    sim >= 0.90 ~ "high",
    sim >= 0.82 ~ "medium",
    TRUE ~ "low"
  )
  
  list(ok = ok, confidence = conf, sec_company_name = sec_company, sim = sim, ticker_match = tmatch)
}

pick_best_cik <- function(candidates, target_name_clean, target_ticker_clean, ua, delay = 0.2) {
  if (nrow(candidates) == 0) return(list(cik = NA_character_, confidence = NA_character_, sec_company_name = NA_character_))
  
  # rank by name similarity to SEC-reported name (via submissions), fallback on sec_name text
  scored <- candidates %>%
    dplyr::mutate(
      sec_name_clean = clean_name_basic(sec_name),
      sim_text = purrr::map_dbl(sec_name_clean, ~name_similarity_jw(target_name_clean, .x))
    ) %>%
    dplyr::arrange(dplyr::desc(sim_text))
  
  # evaluate top K with submissions validation (K small to control calls)
  topk <- head(scored, 5)
  
  best <- list(cik = NA_character_, confidence = "reject", sec_company_name = NA_character_)
  for (i in seq_len(nrow(topk))) {
    cik10 <- topk$cik[i]
    v <- score_candidate(target_name_clean, target_ticker_clean, cik10, ua = ua, delay = delay)
    if (isTRUE(v$ok)) {
      best <- list(cik = cik10, confidence = v$confidence, sec_company_name = v$sec_company_name)
      break
    }
  }
  best
}
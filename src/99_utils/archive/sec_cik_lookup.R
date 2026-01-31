# =============================================================================
# src/99_utils/sec_cik_lookup.R
# =============================================================================
# Browse-EDGAR discovery + Submissions validation utilities.
#
# Key design choices:
# - Candidate discovery (Browse-EDGAR) is permissive.
# - Acceptance is conservative: validate via submissions JSON.
# - Provide confidence tiers and support manual review.
# - Optional: incrementally append validated tickers from submissions into
#   a local historical ticker DB (deterministic).
# =============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(tibble)
  library(purrr)
  library(stringdist)
  library(jsonlite)
  library(rvest)
  library(xml2)
})

# --- Formatting / cleaning ----------------------------------------------------

format_cik_10 <- function(x) {
  sapply(x, function(cik) {
    if (is.na(cik) || cik == "") return(NA_character_)
    cik <- trimws(as.character(cik))
    cik <- gsub("\\D+", "", cik)
    if (nchar(cik) == 0) return(NA_character_)
    sprintf("%010s", cik)
  }, USE.NAMES = FALSE)
}

# Less destructive cleaning: keep dot/dash; strip Refinitiv-like suffixes via variants instead.
clean_ticker_for_lookup <- function(t) {
  sapply(t, function(ticker) {
    if (is.na(ticker)) return(NA_character_)
    ticker <- toupper(trimws(as.character(ticker)))
    if (ticker == "") return(NA_character_)
    ticker <- gsub("\\s+", "", ticker)
    ticker <- sub("[:].*$", "", ticker)              # remove :US / :N etc
    ticker <- gsub("[^A-Z0-9\\.\\-]", "", ticker)    # keep . and -
    if (ticker == "") NA_character_ else ticker
  }, USE.NAMES = FALSE)
}

# Generate lookup variants to improve coverage without lowering quality
ticker_variants <- function(ticker_clean) {
  if (is.na(ticker_clean) || ticker_clean == "") return(character())
  t <- toupper(trimws(as.character(ticker_clean)))
  
  # common “exchange suffix” patterns like IBM.N, VOD.L, ABC.OQ
  strip_dot_suffix <- function(x) {
    if (!grepl("\\.", x)) return(character())
    suffix <- sub("^.*\\.", "", x)
    if (nchar(suffix) <= 3 && grepl("^[A-Z]{1,3}$", suffix)) return(sub("\\.[A-Z]{1,3}$", "", x))
    character()
  }
  
  v <- c(
    t,
    sub("[:].*$", "", t),
    strip_dot_suffix(t),
    gsub("\\.", "-", t),
    gsub("-", ".", t),
    gsub("\\.", "", t)  # last resort variant
  )
  
  v <- unique(v)
  v <- v[!is.na(v) & nzchar(v)]
  v
}

clean_name_basic <- function(x) {
  sapply(x, function(name) {
    name <- toupper(trimws(as.character(name)))
    if (is.na(name) || name == "") return("")
    name <- gsub("\\(.*?\\)", " ", name)
    name <- gsub("&", " AND ", name)
    name <- gsub("[^A-Z0-9 ]", " ", name)
    name <- gsub("\\s+", " ", name)
    name <- trimws(name)
    
    drop <- c("INC", "INCORPORATED", "CORP", "CORPORATION", "CO", "COMPANY",
              "LTD", "LIMITED", "PLC", "LLC", "LP", "HOLDINGS", "HLDGS",
              "GROUP")
    toks <- unlist(strsplit(name, " "))
    toks <- toks[!(toks %in% drop)]
    trimws(paste(toks, collapse = " "))
  }, USE.NAMES = FALSE)
}

name_similarity_jw <- function(a, b) {
  if (length(a) != length(b)) stop("Vectors must be same length")
  sapply(seq_along(a), function(i) {
    if (is.na(a[i]) || is.na(b[i]) || a[i] == "" || b[i] == "") return(0)
    1 - stringdist::stringdist(a[i], b[i], method = "jw", p = 0.1)
  })
}

# --- Schema helpers -----------------------------------------------------------

std_candidate_schema <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(tibble(cik = character(), sec_name = character()))
  df <- as_tibble(df)
  
  if (!"cik" %in% names(df)) {
    if ("cik_str" %in% names(df)) df <- df %>% rename(cik = cik_str)
    if ("cik10" %in% names(df)) df <- df %>% rename(cik = cik10)
  }
  if ("cik" %in% names(df)) df <- df %>% mutate(cik = format_cik_10(cik))
  
  if (!"sec_name" %in% names(df)) {
    alt <- intersect(names(df), c("title", "name", "company_name", "sec_company_name"))
    if (length(alt) > 0) df <- df %>% rename(sec_name = all_of(alt[1])) else df$sec_name <- NA_character_
  }
  
  df %>%
    select(any_of(c("cik", "sec_name"))) %>%
    filter(!is.na(cik)) %>%
    distinct(cik, .keep_all = TRUE)
}

# --- Browse-EDGAR discovery ---------------------------------------------------

browse_edgar_lookup <- function(term,
                                ua,
                                delay = 0.2,
                                cache_dir = "data/interim/sec_cache/browse_edgar") {
  term <- trimws(as.character(term))
  if (is.na(term) || term == "") return(tibble(cik = character(), sec_name = character()))
  
  cache_file <- cache_path_for_key(cache_dir, paste0("browse_", term))
  cached <- cache_read(cache_file)
  if (!is.null(cached)) return(std_candidate_schema(cached))
  
  base_url <- "https://www.sec.gov/cgi-bin/browse-edgar"
  q <- list(action = "getcompany", CIK = term, owner = "exclude", count = "100")
  
  out <- NULL
  
  # 1) ATOM (often resolves single best hit)
  atom_try <- try({
    resp <- sec_get(base_url, query = c(q, list(output = "atom")), ua = ua, delay = delay)
    raw <- httr::content(resp, as = "raw")
    txt <- rawToChar(raw)
    doc <- xml2::read_xml(txt)
    
    cik_node  <- xml2::xml_find_first(doc, ".//*[local-name()='company-info']/*[local-name()='cik']")
    name_node <- xml2::xml_find_first(doc, ".//*[local-name()='company-info']/*[local-name()='conformed-name']")
    
    cik  <- if (!is.na(cik_node))  xml2::xml_text(cik_node)  else NA_character_
    name <- if (!is.na(name_node)) xml2::xml_text(name_node) else NA_character_
    
    res <- tibble(
      cik = format_cik_10(cik),
      sec_name = as.character(name)
    ) %>% filter(!is.na(cik))
    
    if (nrow(res) > 0) res else NULL
  }, silent = TRUE)
  
  if (!inherits(atom_try, "try-error")) out <- atom_try
  
  # 2) HTML parse (handles multi-hit name searches)
  if (is.null(out)) {
    html_try <- try(sec_get(base_url, query = q, ua = ua, delay = delay), silent = TRUE)
    if (!inherits(html_try, "try-error")) {
      html <- httr::content(html_try, as = "text", encoding = "UTF-8")
      page <- rvest::read_html(html)
      
      links <- rvest::html_elements(page, "a")
      href  <- rvest::html_attr(links, "href")
      text  <- rvest::html_text2(links)
      
      keep <- !is.na(href) & grepl("browse-edgar\\?", href) & grepl("CIK=", href)
      href <- href[keep]
      text <- text[keep]
      
      cik <- stringr::str_match(href, "CIK=([0-9]{1,10})")[, 2]
      
      out <- tibble(
        cik = format_cik_10(cik),
        sec_name = text
      ) %>%
        filter(!is.na(cik)) %>%
        distinct(cik, .keep_all = TRUE)
    }
  }
  
  if (is.null(out)) out <- tibble(cik = character(), sec_name = character())
  
  cache_write(cache_file, out)
  std_candidate_schema(out)
}

browse_candidates_ticker_then_name <- function(ticker_clean,
                                               name_clean,
                                               ua,
                                               delay = 0.2,
                                               cache_dir = "data/interim/sec_cache/browse_edgar") {
  out <- tibble(cik = character(), sec_name = character())
  
  tv <- ticker_variants(ticker_clean)
  if (length(tv) > 0) {
    for (q in tv) {
      out <- browse_edgar_lookup(q, ua = ua, delay = delay, cache_dir = cache_dir)
      out <- std_candidate_schema(out)
      if (nrow(out) > 0) break
    }
  }
  
  if (nrow(out) == 0 && !is.na(name_clean) && name_clean != "") {
    out <- browse_edgar_lookup(name_clean, ua = ua, delay = delay, cache_dir = cache_dir)
    out <- std_candidate_schema(out)
  }
  
  out
}

# --- Submissions validation ---------------------------------------------------

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

submissions_has_ticker <- function(sub, target_ticker_clean) {
  if (is.null(sub)) return(FALSE)
  if (is.na(target_ticker_clean) || target_ticker_clean == "") return(FALSE)
  if (is.null(sub$tickers)) return(FALSE)
  tks <- toupper(as.character(sub$tickers))
  target_ticker_clean %in% tks
}

append_historical_mapping <- function(sub, cik10, out_rds) {
  if (is.null(out_rds) || is.na(out_rds) || out_rds == "") return(invisible(FALSE))
  if (is.null(sub) || is.null(sub$tickers)) return(invisible(FALSE))
  
  tks <- unique(toupper(as.character(sub$tickers)))
  tks <- tks[!is.na(tks) & nzchar(tks)]
  if (length(tks) == 0) return(invisible(FALSE))
  
  sec_nm <- NA_character_
  if (!is.null(sub$companyName)) sec_nm <- as.character(sub$companyName)
  if (!is.null(sub$name) && (is.na(sec_nm) || sec_nm == "")) sec_nm <- as.character(sub$name)
  
  new_rows <- tibble(
    ticker_clean = tks,
    cik = format_cik_10(cik10),
    sec_company_name = sec_nm
  ) %>% distinct(ticker_clean, .keep_all = TRUE)
  
  if (file.exists(out_rds)) {
    old <- try(readRDS(out_rds), silent = TRUE)
    if (!inherits(old, "try-error") && is.data.frame(old)) {
      old <- as_tibble(old)
      if ("sec_name" %in% names(old) && !"sec_company_name" %in% names(old)) {
        old <- old %>% rename(sec_company_name = sec_name)
      }
      if (!"ticker_clean" %in% names(old) && "ticker" %in% names(old)) {
        old <- old %>% mutate(ticker_clean = clean_ticker_for_lookup(ticker))
      }
      if (!"cik" %in% names(old)) {
        if ("cik10" %in% names(old)) old <- old %>% rename(cik = cik10)
        if ("cik_str" %in% names(old)) old <- old %>% rename(cik = cik_str)
      }
      old <- old %>% mutate(cik = format_cik_10(cik))
    } else {
      old <- tibble(ticker_clean = character(), cik = character(), sec_company_name = character())
    }
  } else {
    old <- tibble(ticker_clean = character(), cik = character(), sec_company_name = character())
  }
  
  combined <- bind_rows(old, new_rows) %>%
    mutate(ticker_clean = clean_ticker_for_lookup(ticker_clean)) %>%
    distinct(ticker_clean, .keep_all = TRUE)
  
  dir.create(dirname(out_rds), recursive = TRUE, showWarnings = FALSE)
  saveRDS(combined, out_rds)
  invisible(TRUE)
}

score_candidate <- function(target_name_clean,
                            target_ticker_clean,
                            cik10,
                            ua,
                            delay = 0.2,
                            min_name_sim = 0.75,
                            cache_dir = "data/interim/sec_cache/submissions") {
  sub <- sec_submissions_get(cik10, ua = ua, delay = delay, cache_dir = cache_dir)
  if (is.null(sub)) return(list(ok = FALSE, confidence = "reject", sec_company_name = NA_character_, sim = NA_real_, ticker_match = FALSE, sub = NULL))
  
  sec_company <- NA_character_
  if (!is.null(sub$companyName)) sec_company <- as.character(sub$companyName)
  if (!is.null(sub$name) && (is.na(sec_company) || sec_company == "")) sec_company <- as.character(sub$name)
  
  sec_name_clean <- clean_name_basic(sec_company)[1]
  sim <- name_similarity_jw(target_name_clean, sec_name_clean)[1]
  
  tmatch <- submissions_has_ticker(sub, target_ticker_clean)
  
  # conservative: if ticker available, prefer ticker match; otherwise allow strong name match
  ok <- if (!is.na(target_ticker_clean) && target_ticker_clean != "") {
    isTRUE(tmatch) || (!is.na(sim) && sim >= max(min_name_sim, 0.92))
  } else {
    !is.na(sim) && sim >= max(min_name_sim, 0.90)
  }
  
  conf <- dplyr::case_when(
    !ok ~ "reject",
    tmatch ~ "high",
    !is.na(sim) & sim >= 0.95 ~ "high",
    !is.na(sim) & sim >= 0.90 ~ "medium",
    TRUE ~ "low"
  )
  
  list(ok = ok, confidence = conf, sec_company_name = sec_company, sim = sim, ticker_match = tmatch, sub = sub)
}

pick_best_cik <- function(candidates,
                          target_name_clean,
                          target_ticker_clean,
                          ua,
                          delay = 0.2,
                          min_name_sim = 0.75,
                          cache_submissions_dir = "data/interim/sec_cache/submissions",
                          incremental_historical_db_path = NULL) {
  
  cand <- std_candidate_schema(candidates)
  if (nrow(cand) == 0) return(list(cik = NA_character_, confidence = "reject", sec_company_name = NA_character_))
  
  # pre-rank by text similarity to reduce submissions calls
  scored_text <- cand %>%
    mutate(
      sec_name_clean = clean_name_basic(sec_name),
      sim_text = name_similarity_jw(rep(target_name_clean, n()), sec_name_clean)
    ) %>%
    arrange(desc(sim_text))
  
  topk <- head(scored_text, 5)
  
  best <- list(cik = NA_character_, confidence = "reject", sec_company_name = NA_character_)
  best_sub <- NULL
  
  for (i in seq_len(nrow(topk))) {
    cik10 <- topk$cik[i]
    v <- score_candidate(
      target_name_clean = target_name_clean,
      target_ticker_clean = target_ticker_clean,
      cik10 = cik10,
      ua = ua,
      delay = delay,
      min_name_sim = min_name_sim,
      cache_dir = cache_submissions_dir
    )
    
    if (isTRUE(v$ok)) {
      best <- list(cik = cik10, confidence = v$confidence, sec_company_name = v$sec_company_name)
      best_sub <- v$sub
      break
    }
  }
  
  # deterministic incremental enhancement: append SEC-declared tickers for the validated CIK
  if (!is.null(best_sub) && !is.na(best$cik) && best$cik != "reject") {
    try(append_historical_mapping(best_sub, best$cik, incremental_historical_db_path), silent = TRUE)
  }
  
  best
}
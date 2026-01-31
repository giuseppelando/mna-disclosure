# =============================================================================
# src/99_utils/sec_http.R
# =============================================================================
# HTTP + caching helpers for SEC endpoints.
# =============================================================================

suppressPackageStartupMessages({
  library(httr)
})

sec_user_agent <- function() {
  ua <- Sys.getenv("SEC_USER_AGENT")
  if (is.na(ua) || ua == "") {
    ua <- "Giuseppe Lando (Bocconi MSc Finance thesis, giuseppe.lando@studbocconi.it)"
    message("SEC_USER_AGENT not set in environment. Using fallback User-Agent.")
  }
  ua
}

sec_get <- function(url, query = list(), ua = sec_user_agent(), delay = 0.2) {
  resp <- httr::GET(
    url = url,
    query = query,
    httr::add_headers(
      `User-Agent` = ua,
      `Accept` = "*/*",
      `Accept-Encoding` = "gzip, deflate"
    )
  )
  Sys.sleep(delay)
  httr::stop_for_status(resp)
  resp
}

cache_path_for_key <- function(cache_dir, key) {
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)
  safe <- gsub("[^A-Za-z0-9]+", "_", tolower(key))
  safe <- substr(safe, 1, 140)
  file.path(cache_dir, paste0(safe, ".rds"))
}

cache_read <- function(path) {
  if (file.exists(path)) {
    tryCatch(readRDS(path), error = function(e) NULL)
  } else {
    NULL
  }
}

cache_write <- function(path, obj) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  saveRDS(obj, path)
  invisible(path)
}
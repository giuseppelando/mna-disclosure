# src/utils/sec_http.R

sec_user_agent <- function() {
  ua <- Sys.getenv("SEC_USER_AGENT")
  
  if (ua == "") {
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
      Accept = "*/*"
    )
  )
  Sys.sleep(delay)
  httr::stop_for_status(resp)
  resp
}

cache_path_for_key <- function(cache_dir, key) {
  if (!dir.exists(cache_dir)) dir.create(cache_dir, recursive = TRUE)
  safe <- gsub("[^A-Za-z0-9]+", "_", tolower(key))
  safe <- substr(safe, 1, 120)
  file.path(cache_dir, paste0(safe, ".rds"))
}

cache_read <- function(path) {
  if (file.exists(path)) readRDS(path) else NULL
}

cache_write <- function(path, obj) {
  saveRDS(obj, path)
  invisible(path)
}
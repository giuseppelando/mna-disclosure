# src/10_ingest/read_deals.R
suppressPackageStartupMessages({
  library(readxl); library(janitor); library(dplyr); library(stringr); library(lubridate); library(readr); library(rlang)
})


# Trova automaticamente il primo xlsx in data/raw
raw_files <- list.files("data/raw", pattern = ".xlsx$", full.names = TRUE)
stopifnot(length(raw_files) >= 1)
raw_path <- raw_files[1]


message("Leggo: ", raw_path)


df <- readxl::read_excel(raw_path, sheet = 1) |>
  janitor::clean_names()


# Rinominazioni morbide (adatta se i nomi cambiano)
# helper: rinomina solo se la colonna esiste
safe_rename <- function(.data, old, new) {
  if (old %in% names(.data)) {
    dplyr::rename(.data, !!new := !!sym(old))
  } else {
    .data
  }
}

# rimuovi eventuali colonne "unnamed"
df <- df |>
  dplyr::select(!dplyr::starts_with("unnamed"))

# rinomina in modo robusto (solo se le colonne esistono)
df <- df |>
  safe_rename("acquiror_country_code", "acquiror_country") |>
  safe_rename("target_country_code",   "target_country")   |>
  safe_rename("last_deal_status_date", "last_status_date") |>
  safe_rename("target_ticker_symbol",  "target_ticker")

# se manca deal_number, crealo
if (!"deal_number" %in% names(df)) {
  df$deal_number <- seq_len(nrow(df))
}

# trova la colonna del valore (nome variabile può variare)
val_col <- names(df)[grepl("^deal_value", names(df))]
if (length(val_col) >= 1 && !"deal_value_th_usd" %in% names(df)) {
  df <- dplyr::rename(df, deal_value_th_usd = !!sym(val_col[1]))
}



# Coercizioni
clean_num <- function(x) {
  x |> as.character() |> stringr::str_replace_all(",", "") |> as.numeric()
}


if ("deal_value_th_usd" %in% names(df)) {
  df <- df |> mutate(deal_value_th_usd = clean_num(deal_value_th_usd))
}


if ("last_status_date" %in% names(df)) {
  df <- df |> mutate(last_status_date = as.Date(last_status_date))
}


# Flag completamento e normalizzazione ticker
if ("deal_status" %in% names(df)) {
  df <- df |> mutate(deal_completed = if_else(str_to_lower(deal_status) == "completed", 1L, 0L, missing = 0L))
}


df <- df |>
  mutate(target_ticker = toupper(trimws(target_ticker)))


# Tieni solo righe con info minime utili
keep <- df |> filter(!is.na(target_ticker) | !is.na(target_name))


fs::dir_create("data/interim")
readr::write_csv(keep, "data/interim/deals_clean.csv")


message("Salvato → data/interim/deals_clean.csv (", nrow(keep), " righe)")

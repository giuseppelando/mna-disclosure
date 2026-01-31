# =============================================================================
# ANALISI SEVERA - Solo deals con TUTTI i dati necessari per RQ
# =============================================================================

library(dplyr)
library(glue)

deals <- readRDS("C:/Users/giuse/Documents/GitHub/mna-disclosure/data/processed/deals_restricted.rds")

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║            ANALISI SEVERA - COMPLETENESS REALE                   ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")
cat("\n")

n_total <- nrow(deals)
cat(glue("Sample iniziale: {n_total} deals\n\n"))

# =============================================================================
# VERIFICA REALE: Premium calculable
# =============================================================================

cat("═══════════════════════════════════════════════════════════════════\n")
cat("1. PREMIUM CALCULABILITY (RQ1 - CRITICAL)\n")
cat("═══════════════════════════════════════════════════════════════════\n\n")

# Per calcolare premium serve:
# - Offer price (numerator)
# - Target price pre-announcement (denominator)

# Offer price columns (almeno 1)
offer_price_cols <- c(
  "share_price_paid_by_acquiror_for_target_shares_usd",
  "consideration_price_per_share_usd"
)

# Target price columns (preferibilmente 1-week)
target_price_cols <- c(
  "target_share_price_1_week_prior_to_announcement_usd",
  "target_share_price_1_day_prior_to_announcement_usd",
  "target_share_price_4_weeks_prior_to_announcement_usd"
)

# Check availability
cat("Offer price availability:\n")
for(col in offer_price_cols) {
  if(col %in% names(deals)) {
    n_avail <- sum(!is.na(deals[[col]]))
    pct <- round(100 * n_avail / n_total, 1)
    cat(glue("  {col}: {n_avail} ({pct}%)\n"))
  }
}

cat("\nTarget price availability:\n")
for(col in target_price_cols) {
  if(col %in% names(deals)) {
    n_avail <- sum(!is.na(deals[[col]]))
    pct <- round(100 * n_avail / n_total, 1)
    cat(glue("  {col}: {n_avail} ({pct}%)\n"))
  }
}

# Create premium calculability flag
deals <- deals %>%
  mutate(
    # Offer price available (primary or fallback)
    has_offer_price = !is.na(share_price_paid_by_acquiror_for_target_shares_usd) |
                      !is.na(consideration_price_per_share_usd),
    
    # Target price available (any window)
    has_target_price = !is.na(target_share_price_1_week_prior_to_announcement_usd) |
                       !is.na(target_share_price_1_day_prior_to_announcement_usd) |
                       !is.na(target_share_price_4_weeks_prior_to_announcement_usd),
    
    # Premium calculable
    premium_calculable = has_offer_price & has_target_price
  )

n_premium_calc <- sum(deals$premium_calculable)
cat(glue("\n✓ Premium calculable: {n_premium_calc} deals ({round(100*n_premium_calc/n_total,1)}%)\n"))
cat(glue("✗ Premium NOT calculable: {n_total - n_premium_calc} deals\n"))

# Example of non-calculable deal
non_calc <- deals %>% 
  filter(!premium_calculable) %>%
  select(deal_id, target_name, date_announced, 
         share_price_paid_by_acquiror_for_target_shares_usd,
         target_share_price_1_week_prior_to_announcement_usd) %>%
  head(5)

cat("\nEsempi deals NON calculable:\n")
print(non_calc)

# =============================================================================
# FILTRO 1: Premium calculable (CRITICAL)
# =============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║ FILTRO 1: Premium calculable (ESSENZIALE per RQ1)               ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")

deals_f1 <- deals %>% filter(premium_calculable)
n_f1 <- nrow(deals_f1)
cat(glue("\nRetained: {n_f1} deals ({round(100*n_f1/n_total,1)}%)\n"))
cat(glue("Removed: {n_total - n_f1} deals\n"))

# =============================================================================
# FILTRO 2: Payment method (CRITICAL per controls)
# =============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║ FILTRO 2: Payment method presente                                ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")

deals_f2 <- deals_f1 %>%
  filter(!is.na(payment_method_clean) | 
         (!is.na(percentage_of_cash) | !is.na(percentage_of_stock)))

n_f2 <- nrow(deals_f2)
cat(glue("\nRetained: {n_f2} deals ({round(100*n_f2/n_f1,1)}% of F1)\n"))
cat(glue("Cumulative retention: {round(100*n_f2/n_total,1)}% of original\n"))

# =============================================================================
# FILTRO 3: Deal value ≥ $10M (materiality)
# =============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║ FILTRO 3: Deal value ≥ $10M                                      ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")

deals_f3 <- deals_f2 %>%
  filter(!is.na(deal_value_usd_millions) & deal_value_usd_millions >= 10)

n_f3 <- nrow(deals_f3)
cat(glue("\nRetained: {n_f3} deals ({round(100*n_f3/n_f2,1)}% of F2)\n"))
cat(glue("Cumulative retention: {round(100*n_f3/n_total,1)}% of original\n"))

# Deal value distribution
if(n_f3 > 0) {
  cat("\nDeal value distribution (dopo filtro):\n")
  cat(glue("  Min: ${round(min(deals_f3$deal_value_usd_millions),1)}M\n"))
  cat(glue("  Median: ${round(median(deals_f3$deal_value_usd_millions),1)}M\n"))
  cat(glue("  Q3: ${round(quantile(deals_f3$deal_value_usd_millions, 0.75),1)}M\n"))
  cat(glue("  Max: ${round(max(deals_f3$deal_value_usd_millions),1)}M\n"))
}

# =============================================================================
# FILTRO 4 (OPZIONALE): Completion status clear
# =============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║ FILTRO 4: Completion status chiaro (per RQ2)                     ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")

# Check completion clarity
if("deal_completed" %in% names(deals_f3)) {
  n_completed <- sum(deals_f3$deal_completed == 1, na.rm = TRUE)
  n_withdrawn <- sum(deals_f3$deal_completed == 0, na.rm = TRUE)
  n_unclear <- sum(is.na(deals_f3$deal_completed))
  
  cat(glue("\nCompletion status:\n"))
  cat(glue("  Completed: {n_completed}\n"))
  cat(glue("  Withdrawn: {n_withdrawn}\n"))
  cat(glue("  Unclear: {n_unclear}\n"))
  
  if(n_unclear > 0) {
    deals_f4 <- deals_f3 %>% filter(!is.na(deal_completed))
    n_f4 <- nrow(deals_f4)
    cat(glue("\n⚠ Removing {n_unclear} deals with unclear outcome\n"))
  } else {
    deals_f4 <- deals_f3
    n_f4 <- n_f3
    cat("\n✓ All deals have clear completion status\n")
  }
} else {
  deals_f4 <- deals_f3
  n_f4 <- n_f3
  cat("\n✓ Completion already filtered in restrictions\n")
}

cat(glue("\nRetained: {n_f4} deals ({round(100*n_f4/n_f3,1)}% of F3)\n"))
cat(glue("Cumulative retention: {round(100*n_f4/n_total,1)}% of original\n"))

# =============================================================================
# FILTRO 5 (OPZIONALE): Periodo temporale sensato
# =============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║ FILTRO 5: Periodo 2006-2023 (Item 1A obbligatorio post-2005)    ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")

if("year_announced" %in% names(deals_f4)) {
  year_dist <- table(deals_f4$year_announced)
  cat("\nDistribuzione per anno:\n")
  print(year_dist)
  
  # Filter 2006-2023 (Item 1A Risk Factors obbligatorio da 2005)
  deals_f5 <- deals_f4 %>%
    filter(year_announced >= 2006 & year_announced <= 2023)
  
  n_f5 <- nrow(deals_f5)
  n_removed <- n_f4 - n_f5
  
  cat(glue("\n⚠ Removing {n_removed} deals fuori periodo 2006-2023\n"))
  cat(glue("  Rationale: Item 1A obbligatorio post-2005, dati recenti fino 2023\n"))
} else {
  deals_f5 <- deals_f4
  n_f5 <- n_f4
}

cat(glue("\nRetained: {n_f5} deals ({round(100*n_f5/n_f4,1)}% of F4)\n"))
cat(glue("Cumulative retention: {round(100*n_f5/n_total,1)}% of original\n"))

# =============================================================================
# FILTRO 6 (OPZIONALE SEVERO): Deal value ≥ $50M
# =============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║ FILTRO 6 (OPZIONALE): Deal value ≥ $50M (major deals)           ║\n")
cat("╚══════════════════════════════════════════════════════════════════╝\n")

deals_f6 <- deals_f5 %>%
  filter(deal_value_usd_millions >= 50)

n_f6 <- nrow(deals_f6)
cat(glue("\nSe applicato: {n_f6} deals ({round(100*n_f6/n_f5,1)}% of F5)\n"))
cat(glue("Cumulative retention: {round(100*n_f6/n_total,1)}% of original\n"))
cat(glue("Rationale: Major deals = disclosure più ricca, più rilevanti\n"))

# =============================================================================
# SUMMARY E RACCOMANDAZIONI
# =============================================================================

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║                    SUMMARY FILTRI APPLICATI                      ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")
cat(sprintf("║ Sample iniziale:              %-31d ║\n", n_total))
cat("║                                                                  ║\n")
cat(sprintf("║ F1: Premium calculable:       %-18d (%5.1f%%) ║\n", 
            n_f1, 100*n_f1/n_total))
cat(sprintf("║ F2: + Payment method:         %-18d (%5.1f%%) ║\n", 
            n_f2, 100*n_f2/n_total))
cat(sprintf("║ F3: + Deal value ≥ $10M:      %-18d (%5.1f%%) ║\n", 
            n_f3, 100*n_f3/n_total))
cat(sprintf("║ F4: + Completion clear:       %-18d (%5.1f%%) ║\n", 
            n_f4, 100*n_f4/n_total))
cat(sprintf("║ F5: + Periodo 2006-2023:      %-18d (%5.1f%%) ║\n", 
            n_f5, 100*n_f5/n_total))
cat("║                                                                  ║\n")
cat(sprintf("║ F6 (opzionale): ≥ $50M:       %-18d (%5.1f%%) ║\n", 
            n_f6, 100*n_f6/n_total))
cat("╚══════════════════════════════════════════════════════════════════╝\n")

cat("\n")
cat("╔══════════════════════════════════════════════════════════════════╗\n")
cat("║                    RACCOMANDAZIONI FINALI                        ║\n")
cat("╠══════════════════════════════════════════════════════════════════╣\n")

if(n_f5 >= 2000) {
  cat("║ RACCOMANDAZIONE: Usa F1-F5 (base severa)                        ║\n")
  cat(sprintf("║   Sample: %-54d ║\n", n_f5))
  cat("║   • Tutti dati RQ1 + RQ2 presenti                                ║\n")
  cat("║   • Deals materiali (≥$10M)                                      ║\n")
  cat("║   • Periodo sensato per 10-K parsing                             ║\n")
  output_file <- "deals_restricted_STRICT.rds"
  deals_final <- deals_f5
} else if(n_f6 >= 1500) {
  cat("║ RACCOMANDAZIONE: Usa F1-F6 (ultra severa, major deals)          ║\n")
  cat(sprintf("║   Sample: %-54d ║\n", n_f6))
  cat("║   • Major deals only (≥$50M)                                     ║\n")
  cat("║   • Disclosure più ricca                                         ║\n")
  cat("║   • Sample più omogeneo                                          ║\n")
  output_file <- "deals_restricted_MAJOR.rds"
  deals_final <- deals_f6
} else {
  cat("║ WARNING: Sample troppo ridotto con filtri severi                ║\n")
  cat("║ RACCOMANDAZIONE: Usa F1-F4 (moderata)                           ║\n")
  cat(sprintf("║   Sample: %-54d ║\n", n_f4))
  output_file <- "deals_restricted_MODERATE.rds"
  deals_final <- deals_f4
}

cat("╚══════════════════════════════════════════════════════════════════╝\n")

# =============================================================================
# SAVE OUTPUT
# =============================================================================

cat("\n")
cat("Salvando sample finale...\n")

output_path <- paste0("C:/Users/giuse/Documents/GitHub/mna-disclosure/data/processed/", output_file)
saveRDS(deals_final, output_path)
cat(glue("✓ Salvato: {output_file}\n"))

# Export ticker list
ticker_list <- deals_final %>%
  select(deal_id, target_name, target_ticker, date_announced, 
         deal_value_usd_millions, year_announced) %>%
  arrange(target_name)

csv_file <- gsub(".rds", "_ticker_list.csv", output_file)
csv_path <- paste0("C:/Users/giuse/Documents/GitHub/mna-disclosure/data/processed/", csv_file)
write.csv(ticker_list, csv_path, row.names = FALSE)
cat(glue("✓ Salvato: {csv_file}\n"))
cat(glue("  {nrow(ticker_list)} ticker per manual matching\n"))

cat("\n")
cat("═══════════════════════════════════════════════════════════════════\n")
cat("✓ ANALISI COMPLETATA\n")
cat("═══════════════════════════════════════════════════════════════════\n")

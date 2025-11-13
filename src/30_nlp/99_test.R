# src/30_nlp/99_make_analysis_ready.R
# Esempio didattico: crea un dataset pronto per i modelli.

# (In futuro qui farai davvero estrazione + NLP; ora simuliamo.)
dir.create("data/processed", showWarnings = FALSE, recursive = TRUE)

analysis_ready <- data.frame(
  deal_id = 1:5,
  premium = c(0.25, 0.10, 0.18, 0.05, 0.30),              # variabile dipendente (esempio)
  forward_looking = c(0.2, 0.1, 0.4, 0.3, 0.5),           # indice NLP fittizio
  execution_risk  = c(0.3, 0.6, 0.2, 0.4, 0.1),           # indice NLP fittizio
  leverage        = c(0.40, 0.55, 0.20, 0.35, 0.50)       # controllo (esempio)
)

write.csv(analysis_ready, "data/processed/analysis_ready.csv", row.names = FALSE)
message("Creato: data/processed/analysis_ready.csv")

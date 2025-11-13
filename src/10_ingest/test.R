# src/10_ingest/read_datasets.R
# Script di test - lettura dataset di prova

# crea una cartella temporanea per simulare la presenza di dati
dir.create("data/raw", showWarnings = FALSE, recursive = TRUE)

# crea un CSV di esempio
fake_data <- data.frame(
  deal_id = 1:3,
  target = c("A", "B", "C"),
  premium = c(0.25, 0.15, 0.30)
)
write.csv(fake_data, "data/raw/fake_dataset.csv", row.names = FALSE)

# ora "leggiamo" il dataset appena creato
d <- read.csv("data/raw/fake_dataset.csv")

# messaggio di conferma
print("✅ Dataset letto correttamente!")
print(d)

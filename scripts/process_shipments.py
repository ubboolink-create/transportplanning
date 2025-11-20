import pandas as pd
import os
import logging
from datetime import datetime

# -----------------------------------------------------------
# Logging configureren
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

DATA_DIR = "data"
OUTPUT_DIR = "output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "output.csv")

# -----------------------------------------------------------
# Functie: meest recente Excelbestand vinden
# -----------------------------------------------------------
def get_latest_excel_file(directory: str) -> str:
    files = [
        f for f in os.listdir(directory)
        if f.lower().endswith((".xlsx", ".xls"))
    ]

    if not files:
        # FIX: Deze regel was de bron van de IndentationError
        raise FileNotFoundError("Geen Excelbestanden gevonden in /data")

    # sorteer op aanmaaktijd
    files = sorted(
        files,
        key=lambda x: os.path.getmtime(os.path.join(directory, x)),
        reverse=True
    )

    latest_file = os.path.join(directory, files[0])
    logging.info(f"Nieuwste bestand gevonden: {latest_file}")
    return latest_file

# -----------------------------------------------------------
# Functie: dataframe verwerken
# -----------------------------------------------------------
def process_shipments(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met verwerken…")

    # Voorbeeldcriteria opnieuw geïmplementeerd zoals jij vroeg
    # PAS AAN indien je nieuwe criteria hebt

    # Rijen verwijderen waar SKU ontbreekt
    df = df.dropna(subset=["SKU"])

    # Bewerkingen kunnen hier worden uitgebreid
    # Bijvoorbeeld:
    # df = df[df["Quantity"] > 0]

    # Kolommen hernoemen (voorbeeld)
    df = df.rename(columns={
        "SKU": "sku",
        "Description": "description",
        "Quantity": "quantity",
    })

    # Spaties en rare karakters verwijderen in tekstkolommen
    df["sku"] = df["sku"].astype(str).str.strip()
    df["description"] = df["description"].astype(str).str.strip()

    logging.info("Verwerking voltooid.")
    return df

# -----------------------------------------------------------
# Main processing routine
# -----------------------------------------------------------
def main():
    # Output directory aanmaken indien nodig
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Meest recente Excelbestand ophalen
    excel_path = get_latest_excel_file(DATA_DIR)

    # Excelbestand inlezen
    logging.info("Excelbestand wordt geladen…")
    df = pd.read_excel(excel_path)

    # Verwerken
    df_processed = process_shipments(df)

    # Output opslaan
    df_processed.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
    logging.info(f"CSV opgeslagen als: {OUTPUT_FILE}")

# -----------------------------------------------------------
# Script starten
# -----------------------------------------------------------
if __name__ == "__main__":
    main()

import pandas as pd
import os
import logging
from datetime import datetime

# -----------------------------------------------------------
# Configuratie en Setup
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
# Functie: dataframe verwerken (met verbeterde kolom opschoning)
# -----------------------------------------------------------
def process_shipments(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met verwerken en hernoemen van kolommen...")
    
    # --- DEBUGGING STAP: Log de originele headers ---
    logging.info(f"Kolommen na laden: {df.columns.tolist()}") 
    
    # CRUCIALE FIX 1: Verwijder leidende/volgende spaties uit alle kolomnamen
    df.columns = df.columns.str.strip() 

    # CRUCIALE FIX 2: Maak alle kolomnamen lowercase om case-gevoeligheid te elimineren
    df.columns = df.columns.str.lower()
    
    # Hernoem de kolommen die we nodig hebben (de keys zijn nu lowercase).
    df = df.rename(columns={
        "material": "sku",          # Artikelnummer
        "verzenden aan": "shipto",  # Adres code
        "laadmeter": "lm",          # Laadmeter
        "vervoerder": "carrier",    # Vervoerder
    }, errors='ignore') 
    
    # 1. Rijen verwijderen waar Artikelnummer (nu 'sku') ontbreekt.
    df = df.dropna(subset=["sku"]) 
    
    # 2. Opschonen van de tekstkolommen
    df["sku"] = df["sku"].astype(str).str.strip()
    df["shipto"] = df["shipto"].astype(str).str.strip() 

    # 3. Zorg dat LM numeriek is
    df["lm"] = pd.to_numeric(df["lm"], errors='coerce').fillna(0.0)

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

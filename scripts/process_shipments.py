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
# Functie: dataframe verwerken (met definitieve kolomnamen en fallback)
# -----------------------------------------------------------
def process_shipments(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met verwerken en hernoemen van kolommen...")
    
    # 1. Verwijder leidende/volgende spaties uit alle kolomnamen
    df.columns = df.columns.str.strip() 

    # 2. Maak alle kolomnamen lowercase om case-gevoeligheid te elimineren
    df.columns = df.columns.str.lower()
    
    # Hernoem de bekende, niet-lege kolommen
    df = df.rename(columns={
        "verzenden-aan code": "shipto",  # Adres code
        "load meter": "lm",           # Laadmeter
        "vervoerder/ldv": "carrier",  # Vervoerder
    }, errors='ignore') 
    
    # 3. FIX VOOR LEGE SKU-CEL (BK1): Zoek de artikelkolom
    # We proberen eerst de naam 'artikel' die in de log stond
    if "artikel" in df.columns:
        df = df.rename(columns={"artikel": "sku"}, errors='ignore')
    # Zo niet, dan proberen we de meest waarschijnlijke 'Unnamed' kolom in de buurt
    elif "unnamed: 61" in df.columns: # Of probeer 62, 63 als 61 niet werkt
        df = df.rename(columns={"unnamed: 61": "sku"}, errors='ignore')
    else:
        # Debugging stap: Dit zal opnieuw de logs sturen als het niet werkt.
        logging.error("Kan kolom voor Artikelnummer (SKU) niet vinden. Controleer kolomnamen.")


    # 4. Rijen verwijderen waar Artikelnummer (nu 'sku') ontbreekt.
    df = df.dropna(subset=["sku"]) 
    
    # 5. Opschonen van de tekstkolommen
    df["sku"] = df["sku"].astype(str).str.strip()
    df["shipto"] = df["shipto"].astype(str).str.strip() 

    # 6. Zorg dat LM numeriek is
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
    # We blijven bij de default header=0, aangezien je zei dat de tweede rij geen header heeft.
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

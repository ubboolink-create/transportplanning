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
FINAL_OUTPUT_FILE = os.path.join(OUTPUT_DIR, "transportplanning_ready.csv")

# De maximale belading per vrachtwagen (bijvoorbeeld 13.6 Laadmeters)
MAX_LM = 13.6 

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
    
    # Hernoem de bekende kolommen. We gebruiken de kolomnamen uit de laatste succesvolle log.
    df = df.rename(columns={
        "verzenden-aan code": "shipto",  # Adres code (A10)
        "load meter": "lm",           # Laadmeter (A16)
        "vervoerder/ldv": "carrier",  # Vervoerder (A08)
    }, errors='ignore') 
    
    # 3. FIX VOOR LEGE SKU-CEL: Zoek de artikelkolom
    # We proberen de naam 'artikel' en de meest waarschijnlijke 'Unnamed' kolom.
    if "artikel" in df.columns:
        df = df.rename(columns={"artikel": "sku"}, errors='ignore')
    elif "unnamed: 61" in df.columns: # Gevonden in eerdere logs
        df = df.rename(columns={"unnamed: 61": "sku"}, errors='ignore')
    else:
        logging.warning("Kon kolom voor Artikelnummer (SKU) niet vinden. Filtering op SKU is mogelijk onbetrouwbaar.")
        # Voeg een lege kolom 'sku' toe om crashes op te vangen.
        df['sku'] = None


    # 4. Data Opschonen
    df = df.dropna(subset=["sku"]) 
    df["sku"] = df["sku"].astype(str).str.strip()
    df["shipto"] = df["shipto"].astype(str).str.strip() 
    df["carrier"] = df["carrier"].astype(str).str.strip() 
    df["lm"] = pd.to_numeric(df["lm"], errors='coerce').fillna(0.0)
    
    logging.info("Verwerking voltooid.")
    return df

# -----------------------------------------------------------
# Functie: Transportplanning en Groepering
# -----------------------------------------------------------
def perform_transport_planning(df_processed: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met transportplanning: Groeperen en rangschikken...")

    # 1. Groeperen op de drie criteria: Vervoerder (A08), Adres (A10) en SKU
    df_grouped = df_processed.groupby(['carrier', 'shipto', 'sku']).agg(
        # Tel het aantal regels voor deze zending
        num_items=('sku', 'size'),  
        # Bereken de totale laadmeter voor deze groep
        total_lm=('lm', 'sum') 
    ).reset_index()

    # 2. De output voorbereiden
    df_grouped['truck_id'] = None
    df_grouped['lm_used_in_truck'] = 0.0

    # 3. Rangschik de zendingen
    # Sorteer op Vervoerder, dan op Adres, dan op Laadmeter (grootste eerst)
    df_grouped = df_grouped.sort_values(
        ['carrier', 'shipto', 'total_lm'],
        ascending=[True, True, False]
    )
    
    # 4. Laadplan maken (zeer vereenvoudigde versie van bin packing)
    current_truck_id = 1
    current_carrier = None
    current_lm = 0.0

    for index, row in df_grouped.iterrows():
        # Reset de vrachtwagen als de vervoerder wijzigt
        if current_carrier != row['carrier']:
            current_lm = 0.0
            current_truck_id += 1 
            current_carrier = row['carrier']
            logging.info(f"Nieuwe vervoerder ({current_carrier}). Start Truck ID {current_truck_id}")

        # Kan de zending in de huidige vrachtwagen?
        if current_lm + row['total_lm'] <= MAX_LM:
            # Ja, voeg toe
            current_lm += row['total_lm']
            df_grouped.loc[index, 'truck_id'] = f"{row['carrier']}-{current_truck_id}"
            df_grouped.loc[index, 'lm_used_in_truck'] = current_lm
        else:
            # Nee, start een nieuwe vrachtwagen
            current_truck_id += 1
            current_lm = row['total_lm']
            df_grouped.loc[index, 'truck_id'] = f"{row['carrier']}-{current_truck_id}"
            df_grouped.loc[index, 'lm_used_in_truck'] = current_lm
            
    logging.info(f"Planning voltooid. Totaal aantal vrachtwagens: {current_truck_id}")
    return df_grouped

# -----------------------------------------------------------
# Main processing routine
# -----------------------------------------------------------
def main():
    # Output directory aanmaken indien nodig
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Meest recente Excelbestand ophalen
    try:
        excel_path = get_latest_excel_file(DATA_DIR)
    except FileNotFoundError as e:
        logging.error(e)
        return

    # Excelbestand inlezen
    logging.info("Excelbestand wordt geladen…")
    df = pd.read_excel(excel_path) 

    # 1. Verwerken en opschonen van de kolommen
    df_processed = process_shipments(df)

    # 2. Transportplanning uitvoeren
    df_final = perform_transport_planning(df_processed)

    # 3. Output opslaan
    df_final.to_csv(FINAL_OUTPUT_FILE, index=False, encoding="utf-8")
    logging.info(f"Final CSV opgeslagen als: {FINAL_OUTPUT_FILE}")

# -----------------------------------------------------------
# Script starten
# -----------------------------------------------------------
if __name__ == "__main__":
    main()

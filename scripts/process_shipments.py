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

    files = sorted(
        files,
        key=lambda x: os.path.getmtime(os.path.join(directory, x)),
        reverse=True
    )

    latest_file = os.path.join(directory, files[0])
    logging.info(f"Nieuwste bestand gevonden: {latest_file}")
    return latest_file

# -----------------------------------------------------------
# Functie: dataframe verwerken (DEFINITIEF MET NUMERIEKE INDEXEN)
# -----------------------------------------------------------
def process_shipments(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met verwerken (ULTIEME FIX: NUMERIEKE INDEXEN)...")
    logging.info(f"Aantal rijen vóór verwerking: {len(df)}")
    
    # 1. Hernoem de kolommen direct op basis van de absolute index (0-gebaseerd)
    # SKU (BK) = Index 61
    # LM (BS) = Index 70
    # Shipto (AE/Verzenden-aan code) = Index 30 (schatting)
    # Carrier (BX/Vervoerder/LDV) = Index 75 (schatting)
    try:
        df = df.rename(columns={
            30: "shipto", 
            75: "carrier",
            61: "sku",      # BK kolom
            70: "lm"       # BS kolom
        }, errors='raise') # Gebruik 'raise' om te zien of de indices niet bestaan
    except KeyError:
        # Als de indices niet bestaan, is de input leeg of de indexen zijn fout.
        logging.error("Kon niet hernoemen: Controleer of de indices (30, 61, 70, 75) kloppen voor uw Excel.")
        return pd.DataFrame() # Retourneer een leeg DF
    
    logging.info("Kolomnamen succesvol ingesteld met numerieke indexen.")
    
    # 2. Nan-waarden opvullen om crashes in de groepering te voorkomen (FILTER IS NOG STEEDS UIT)
    # LM: Zorg dat het numeriek is
    df['lm'] = pd.to_numeric(df['lm'], errors='coerce').fillna(0.0)

    # SKU: Maak een leesbare placeholder
    if 'sku' in df.columns:
        df['sku'] = df['sku'].astype(str).str.strip().fillna('ONBEKEND_SKU')
    if 'carrier' in df.columns:
        df['carrier'] = df['carrier'].astype(str).str.strip().fillna('ONBEKEND_VERVOERDER')
    if 'shipto' in df.columns:
        df['shipto'] = df['shipto'].astype(str).str.strip().fillna('ONBEKEND_ADRES')
        
    logging.info(f"Aantal rijen na verwerking: {len(df)}") 
    return df

# -----------------------------------------------------------
# Functie: Transportplanning en Groepering (ongewijzigd)
# -----------------------------------------------------------
def perform_transport_planning(df_processed: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met transportplanning: Groeperen en rangschikken...")
    # ... (Planning logica is ongewijzigd) ...
    # Groeperen op de drie criteria
    df_grouped = df_processed.groupby(['carrier', 'shipto', 'sku']).agg(
        num_items=('sku', 'size'),  
        total_lm=('lm', 'sum') 
    ).reset_index()

    df_grouped['truck_id'] = None
    df_grouped['lm_used_in_truck'] = 0.0

    df_grouped = df_grouped.sort_values(
        ['carrier', 'shipto', 'total_lm'],
        ascending=[True, True, False]
    )
    
    current_truck_id = 1
    current_carrier = None
    current_lm = 0.0

    for index, row in df_grouped.iterrows():
        if current_carrier != row['carrier']:
            current_lm = 0.0
            current_truck_id += 1 
            current_carrier = row['carrier']

        if current_lm + row['total_lm'] <= MAX_LM:
            current_lm += row['total_lm']
            df_grouped.loc[index, 'truck_id'] = f"{row['carrier']}-{current_truck_id}"
            df_grouped.loc[index, 'lm_used_in_truck'] = current_lm
        else:
            current_truck_id += 1
            current_lm = row['total_lm']
            df_grouped.loc[index, 'truck_id'] = f"{row['carrier']}-{current_truck_id}"
            df_grouped.loc[index, 'lm_used_in_truck'] = current_lm
            
    logging.info(f"Planning voltooid. Totaal aantal vrachtwagens: {current_truck_id}")
    return df_grouped

# -----------------------------------------------------------
# Main processing routine (ULTIEME FIX)
# -----------------------------------------------------------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        excel_path = get_latest_excel_file(DATA_DIR)
    except FileNotFoundError as e:
        logging.error(e)
        return

    logging.info("Excelbestand wordt geladen…")
    # !!! DE BELANGRIJKE FIX: HEADER=None - Negeer alle headers en gebruik indexen
    df = pd.read_excel(excel_path, header=None) 

    df_processed = process_shipments(df)

    if not df_processed.empty:
        df_final = perform_transport_planning(df_processed)

        df_final.to_csv(FINAL_OUTPUT_FILE, index=False, encoding="utf-8")
        logging.info(f"Final CSV opgeslagen als: {FINAL_OUTPUT_FILE}")
    else:
        logging.error("FATALE FOUT: Het Excel-bestand leest als een leeg DataFrame.")


# -----------------------------------------------------------
# Script starten
# -----------------------------------------------------------
if __name__ == "__main__":
    main()

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
# Functie: dataframe verwerken (LAATSTE FIX HEADER=0)
# -----------------------------------------------------------
def process_shipments(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met verwerken (DEFINITIEF: DYNAMISCHE SKU-FIX)...")
    logging.info(f"Aantal rijen vóór verwerking: {len(df)}")
    
    # 1. Headers opschonen
    df.columns = df.columns.astype(str).str.strip().str.lower()
    
    # 2. Hernoem de bekende kolommen.
    # We gebruiken de bekende namen en de door Pandas gegeven namen ('unnamed')
    # Carrier (CC) = 'unnamed: 80' of 'vervoerder/ldv'
    # Shipto (AH) = 'unnamed: 33' of 'verzenden-aan code'
    # SKU (BK) = 'unnamed: 61'
    # LM (BS) = 'load meter'
    
    df = df.rename(columns={
        'unnamed: 33': "shipto",    # AH
        'unnamed: 80': "carrier",   # CC
        'unnamed: 70': "lm",        # BS
        # Originele namen (als ze wel bestaan)
        'verzenden-aan code': 'shipto',
        'vervoerder/ldv': 'carrier',
        'load meter': 'lm',
    }, errors='ignore') 
    
    # ---!!! DYNAMISCHE FIX VOOR SKU (INDEX 61) !!!---
    sku_col_name = 'unnamed: 61'
    
    if sku_col_name in df.columns:
        df = df.rename(columns={sku_col_name: "sku"}, errors='ignore')
    else:
        # Als 'unnamed: 61' er niet is, print dan alle kolommen voor debug.
        logging.error(f"SKU-kolom '{sku_col_name}' niet gevonden. Beschikbare kolommen: {df.columns.tolist()}")
        # We proberen nu handmatig de 62e kolom te pakken op index, ongeacht de naam:
        if len(df.columns) > 61:
            old_name = df.columns[61]
            df = df.rename(columns={old_name: "sku"}, errors='ignore')
            logging.warning(f"SKU-kolom handmatig hernoemd van '{old_name}' naar 'sku'.")
        else:
            logging.error("Kan de 62e kolom (Index 61 / BK) niet bereiken. DataFrame is te klein.")
            
    logging.info("Kolomnamen succesvol ingesteld.")
    
    # 3. Opschoning
    if 'lm' not in df.columns:
         logging.warning("LM kolom niet gevonden. Instelling op 0.0")
         df['lm'] = 0.0

    df['lm'] = pd.to_numeric(df['lm'], errors='coerce').fillna(0.0)

    # SKU, Carrier, Shipto: Opschonen en filteren
    for col in ['sku', 'carrier', 'shipto']:
        if col not in df.columns:
            logging.error(f"Kritieke kolom '{col}' ontbreekt na hernoeming. Script zal crashen of leeg teruggeven.")
            return pd.DataFrame() 

        df[col] = df[col].astype(str).str.strip()
        
    # ---!!! DE FILTER WORDT AANGEZET !!!---
    if 'sku' in df.columns:
        # Verwijder rijen waar SKU leeg is (ook 'nan' na str.strip())
        df = df.dropna(subset=['sku'])
        df = df[df['sku'] != '']
        
    # 4. Nan-waarden opvullen voor groepering 
    df['sku'] = df['sku'].fillna('ONBEKEND_SKU')
    df['carrier'] = df['carrier'].fillna('ONBEKEND_VERVOERDER')
    df['shipto'] = df['shipto'].fillna('ONBEKEND_ADRES')
        
    logging.info(f"Aantal rijen na filtering: {len(df)}") 
    return df

# -----------------------------------------------------------
# Functie: Transportplanning en Groepering (ongewijzigd)
# -----------------------------------------------------------
def perform_transport_planning(df_processed: pd.DataFrame) -> pd.DataFrame:
    # ... (code ongewijzigd) ...
    logging.info("Start met transportplanning: Groeperen en rangschikken...")
    
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
# Main processing routine
# -----------------------------------------------------------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        excel_path = get_latest_excel_file(DATA_DIR)
    except FileNotFoundError as e:
        logging.error(e)
        return

    logging.info("Excelbestand wordt geladen…")
    # !!! Header is Rij 1 (Index 0) !!!
    df = pd.read_excel(excel_path, header=0) 

    df_processed = process_shipments(df)

    if not df_processed.empty:
        df_final = perform_transport_planning(df_processed)

        df_final.to_csv(FINAL_OUTPUT_FILE, index=False, encoding="utf-8")
        logging.info(f"Final CSV opgeslagen als: {FINAL_OUTPUT_FILE}")
    else:
        logging.error("FATALE FOUT: Geen data overgebleven na filtering. Controleer de log voor de daadwerkelijke naam van de SKU-kolom.")


# -----------------------------------------------------------
# Script starten
# -----------------------------------------------------------
if __name__ == "__main__":
    main()

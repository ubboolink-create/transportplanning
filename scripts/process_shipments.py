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
# Functie: dataframe verwerken (DEBUG FILTER OFF)
# -----------------------------------------------------------
def process_shipments(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met verwerken en hernoemen van kolommen (DEBUG MODE)...")
    logging.info(f"Aantal rijen vóór filtering: {len(df)}")
    
    # 1. & 2. Headers opschonen
    df.columns = df.columns.str.strip().str.lower()
    
    # 3. Hernoem de bekende kolommen.
    df = df.rename(columns={
        "verzenden-aan code": "shipto", 
        "vervoerder/ldv": "carrier",  
    }, errors='ignore') 
    
    # 4. LM FIX: Gebruik de bevestigde Load meter kolom (BS)
    lm_col_name = "load meter"

    if lm_col_name not in df.columns:
        df[lm_col_name] = 0.0
    
    df[lm_col_name] = pd.to_numeric(df[lm_col_name], errors='coerce').fillna(0.0)
    df['lm'] = df[lm_col_name]
    
    # 5. SKU FIX: Gebruik de bevestigde kolom BK (index 61 / unnamed: 61)
    sku_found = False
    
    if "unnamed: 61" in df.columns:
        df = df.rename(columns={"unnamed: 61": "sku"}, errors='ignore')
        sku_found = True
    elif "artikel" in df.columns:
        df = df.rename(columns={"artikel": "sku"}, errors='ignore')
        sku_found = True
            
    if not sku_found:
        logging.warning("Kan kolom voor Artikelnummer (SKU) niet vinden.")
        df['sku'] = None 

    # 6. Data Opschonen
    if 'sku' in df.columns:
        df["sku"] = df["sku"].astype(str).str.strip()
    if 'shipto' in df.columns:
        df["shipto"] = df["shipto"].astype(str).str.strip() 
    if 'carrier' in df.columns:
        df["carrier"] = df["carrier"].astype(str).str.strip() 
        
    # ---!!! FILTER TIJDELIJK UITGESCHAKELD !!!---
    # De data wordt niet gefilterd, zodat we de 'sku' kolom in de output kunnen inspecteren.
    
    logging.info(f"Aantal rijen ná (geen) filtering: {len(df)}") 
    
    # EXTRA DEBUGGING: Log de eerste 5 waarden van de SKU kolom
    if 'sku' in df.columns:
        unique_sku_values = df['sku'].dropna().unique()
        logging.info(f"Eerste 5 unieke, niet-lege SKU waarden: {unique_sku_values[:5].tolist()}")

    logging.info("Verwerking voltooid.")
    return df

# -----------------------------------------------------------
# Functie: Transportplanning en Groepering (onveranderd)
# -----------------------------------------------------------
def perform_transport_planning(df_processed: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met transportplanning: Groeperen en rangschikken...")
    # Groeperen op de drie criteria: Vervoerder (A08), Adres (A10) en SKU
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
    df = pd.read_excel(excel_path) 

    df_processed = process_shipments(df)

    # De planning wordt nu altijd uitgevoerd, zelfs als de SKU's leeg zijn.
    if not df_processed.empty:
        df_final = perform_transport_planning(df_processed)

        df_final.to_csv(FINAL_OUTPUT_FILE, index=False, encoding="utf-8")
        logging.info(f"Final CSV opgeslagen als: {FINAL_OUTPUT_FILE}")
    else:
        # Dit zou nu niet moeten gebeuren, tenzij het Excel-bestand leeg is.
        logging.warning("Het Excel-bestand bevat helemaal geen rijen data.")


# -----------------------------------------------------------
# Script starten
# -----------------------------------------------------------
if __name__ == "__main__":
    main()

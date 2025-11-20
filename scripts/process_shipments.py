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

    files = sorted(
        files,
        key=lambda x: os.path.getmtime(os.path.join(directory, x)),
        reverse=True
    )

    latest_file = os.path.join(directory, files[0])
    logging.info(f"Nieuwste bestand gevonden: {latest_file}")
    return latest_file

# -----------------------------------------------------------
# Functie: dataframe verwerken (Debug-modus)
# -----------------------------------------------------------
def process_shipments(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met verwerken en hernoemen van kolommen...")
    logging.info(f"Aantal rijen vóór filtering: {len(df)}") # DEBUG LOG
    
    # 1. Verwijder leidende/volgende spaties uit alle kolomnamen
    df.columns = df.columns.str.strip() 

    # 2. Maak alle kolomnamen lowercase om case-gevoeligheid te elimineren
    df.columns = df.columns.str.lower()
    
    # Hernoem de bekende kolommen.
    df = df.rename(columns={
        "verzenden-aan code": "shipto", 
        "load meter": "lm",           
        "vervoerder/ldv": "carrier",  
    }, errors='ignore') 
    
    # 3. FIX VOOR LEGE SKU-CEL: Zoek de artikelkolom
    if "artikel" in df.columns:
        df = df.rename(columns={"artikel": "sku"}, errors='ignore')
    elif "unnamed: 61" in df.columns: 
        df = df.rename(columns={"unnamed: 61": "sku"}, errors='ignore')
    else:
        logging.warning("Kan kolom voor Artikelnummer (SKU) niet vinden. SKU-kolom is waarschijnlijk leeg.")
        # Zorg ervoor dat de 'sku' kolom altijd bestaat voor latere stappen
        df['sku'] = None 
    
    # 4. Filter Tijdelijk UITGEZET. Data wordt niet verwijderd.
    #df = df.dropna(subset=["sku"]) 
    
    # 5. Opschonen van de tekstkolommen
    if 'sku' in df.columns:
        df["sku"] = df["sku"].astype(str).str.strip()
    if 'shipto' in df.columns:
        df["shipto"] = df["shipto"].astype(str).str.strip() 
    if 'carrier' in df.columns:
        df["carrier"] = df["carrier"].astype(str).str.strip() 
    
    df["lm"] = pd.to_numeric(df["lm"], errors='coerce').fillna(0.0)

    logging.info(f"Aantal rijen ná filtering (Debug): {len(df)}") # DEBUG LOG
    logging.info("Verwerking voltooid.")
    return df

# -----------------------------------------------------------
# Functie: Transportplanning en Groepering (onveranderd)
# -----------------------------------------------------------
def perform_transport_planning(df_processed: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met transportplanning: Groeperen en rangschikken...")

    # Aangezien we het filter uitzetten, groeperen we alle rijen, zelfs de lege
    df_grouped = df_processed.groupby(['carrier', 'shipto', 'sku']).agg(
        num_items=('sku', 'size'),  
        total_lm=('lm', 'sum') 
    ).reset_index()

    # ... (rest van de planning logic)

    current_truck_id = 1
    current_carrier = None
    current_lm = 0.0

    # Sort the groups based on carrier, shipto, and total_lm (largest first)
    df_grouped = df_grouped.sort_values(
        ['carrier', 'shipto', 'total_lm'],
        ascending=[True, True, False]
    )

    for index, row in df_grouped.iterrows():
        if current_carrier != row['carrier']:
            current_lm = 0.0
            current_truck_id += 1 
            current_carrier = row['carrier']
            logging.info(f"Nieuwe vervoerder ({current_carrier}). Start Truck ID {current_truck_id}")

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
# Main processing routine (onveranderd)
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

    df_final = perform_transport_planning(df_processed)

    df_final.to_csv(FINAL_OUTPUT_FILE, index=False, encoding="utf-8")
    logging.info(f"Final CSV opgeslagen als: {FINAL_OUTPUT_FILE}")

if __name__ == "__main__":
    main()

# DEFINITIEVE FUNCTIE: corrigeert SKU (BK/unnamed: 61) en LM (BS/load meter)

def process_shipments(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met verwerken en hernoemen van kolommen...")
    logging.info(f"Aantal rijen vóór filtering: {len(df)}")
    
    # 1. Verwijder leidende/volgende spaties uit alle kolomnamen
    df.columns = df.columns.str.strip() 

    # 2. Maak alle kolomnamen lowercase om case-gevoeligheid te elimineren
    df.columns = df.columns.str.lower()
    
    # 3. Hernoem de bekende kolommen.
    df = df.rename(columns={
        "verzenden-aan code": "shipto", 
        "vervoerder/ldv": "carrier",  
    }, errors='ignore') 
    
    # --- LM FIX: Gebruik de bevestigde Load meter kolom (BS) ---
    lm_col_name = "load meter"

    if lm_col_name not in df.columns:
        df[lm_col_name] = 0.0 # Valback als de kolom niet bestaat
    
    # Zorg dat de LM waarden numeriek zijn en vul lege cellen met 0.0
    df[lm_col_name] = pd.to_numeric(df[lm_col_name], errors='coerce').fillna(0.0)
    df['lm'] = df[lm_col_name]
    
    # 4. SKU FIX: Gebruik de bevestigde kolom BK (index 61 / unnamed: 61)
    sku_found = False
    
    # We forceren de naam van de kolom BK (index 61)
    if "unnamed: 61" in df.columns:
        df = df.rename(columns={"unnamed: 61": "sku"}, errors='ignore')
        sku_found = True
    elif "artikel" in df.columns:
        # Dit is de kolom BJ, die we als fallback gebruiken.
        df = df.rename(columns={"artikel": "sku"}, errors='ignore')
        sku_found = True
            
    if not sku_found:
        logging.warning("Kan kolom voor Artikelnummer (SKU) niet vinden.")
        df['sku'] = None 

    # 5. Data Opschonen en Filter AANZETTEN
    
    # Opschonen van de tekstkolommen
    if 'sku' in df.columns:
        df["sku"] = df["sku"].astype(str).str.strip()
    if 'shipto' in df.columns:
        df["shipto"] = df["shipto"].astype(str).str.strip() 
    if 'carrier' in df.columns:
        df["carrier"] = df["carrier"].astype(str).str.strip() 
        
    # Filter aanzetten: verwijder alleen rijen waar de SKU leeg is.
    if 'sku' in df.columns:
        df = df.dropna(subset=["sku"]) 
        # Verwijder ook rijen die 'nan' of leeg zijn na stripping
        df = df[~df['sku'].isin(['nan', 'none', ''])]
    
    logging.info(f"Aantal rijen ná filtering: {len(df)}") 
    logging.info("Verwerking voltooid.")
    return df

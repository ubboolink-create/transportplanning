# -----------------------------------------------------------
# Functie: dataframe verwerken
# -----------------------------------------------------------
def process_shipments(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met verwerken en hernoemen van kolommen...")

    # Hernoem alleen de kolommen die we ZEKER nodig hebben en die in je Excel staan.
    # Eventuele onbekende kolommen worden genegeerd (errors='ignore').
    df = df.rename(columns={
        "Material": "sku",          # Artikelnummer
        "Verzenden aan": "shipto",  # Adres code
        "Laadmeter": "lm",          # Laadmeter
        "Vervoerder": "carrier",    # Vervoerder
    }, errors='ignore') 
    
    # 1. Rijen verwijderen waar Artikelnummer (nu 'sku') ontbreekt.
    # Dit is de kritieke regel die nu werkt met de juiste naam.
    df = df.dropna(subset=["sku"]) 
    
    # 2. Opschonen van de tekstkolommen
    df["sku"] = df["sku"].astype(str).str.strip()
    df["shipto"] = df["shipto"].astype(str).str.strip() 

    # 3. Zorg dat LM numeriek is (nodig voor berekeningen)
    df["lm"] = pd.to_numeric(df["lm"], errors='coerce').fillna(0.0)

    logging.info("Verwerking voltooid.")
    return df

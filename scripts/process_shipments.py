# -----------------------------------------------------------
# Functie: dataframe verwerken
# -----------------------------------------------------------
def process_shipments(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Start met verwerken en hernoemen van kolommen...")

    # Hernoem eerst alle relevante kolommen naar de interne, simpele namen.
    # Dit is cruciaal, anders werkt de rest van de logica niet.
    df = df.rename(columns={
        "Material": "sku",          # Artikelnummer (jouw criterium)
        "Verzenden aan": "shipto",  # Adres code
        "Laadmeter": "lm",          # Laadmeter
        "Vervoerder": "carrier",    # Vervoerder
        
        # De volgende kolommen zijn belangrijk voor de output,
        # maar zijn mogelijk niet de juiste namen in jouw bestand. 
        # Voeg ze toe als ze er zijn, of verwijder ze als ze niet bestaan.
        "Order": "orderno",
        "Order Position": "regel",
        "Volgnummer": "volgnummer",
        "Set": "set"
    })
    
    # 1. Rijen verwijderen waar Artikelnummer (nu 'sku') ontbreekt.
    # Dit lost de oorspronkelijke KeyError op, omdat we nu op "sku" zoeken.
    df = df.dropna(subset=["sku"]) 

    # 2. Opschonen van de tekstkolommen
    df["sku"] = df["sku"].astype(str).str.strip()
    df["shipto"] = df["shipto"].astype(str).str.strip() 

    # 3. Zorg dat LM numeriek is (deze stap hoort erbij, ook al stond hij niet in jouw opgeschoonde code)
    df["lm"] = pd.to_numeric(df["lm"], errors='coerce').fillna(0.0)

    logging.info("Verwerking voltooid.")
    return df

# Let op:
# De rest van jouw OORSPRONKELIJKE, LANGE script (dat je aan ChatGPT gaf)
# moet in dit bestand worden toegevoegd na deze functie, 
# anders doet de code niets met de LM-criteria en de vervoerdertoewijzing.

import os
import urllib.request
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def download_file(url, dest_path):
    if os.path.exists(dest_path):
        logging.info(f"✔ El archivo ya existe: {dest_path}")
        return
    
    logging.info(f"⏳ Descargando {url}...")
    try:
        urllib.request.urlretrieve(url, dest_path)
        logging.info(f"✅ Guardado en {dest_path}")
    except Exception as e:
        logging.error(f"❌ Error descargando {url}: {e}")

def main():
    # Rutas basadas en tu estructura ~/ibdn
    base_path = os.path.expanduser("~/ibdn")
    data_path = os.path.join(base_path, "data")
    models_path = os.path.join(base_path, "models")

    # Crear carpetas si no existen
    os.makedirs(data_path, exist_ok=True)
    os.makedirs(models_path, exist_ok=True)

    # Lista de recursos del script original
    resources = [
        # Datos en la carpeta data/
        ("http://s3.amazonaws.com/agile_data_science/simple_flight_delay_features.jsonl.bz2", 
         os.path.join(data_path, "simple_flight_delay_features.jsonl.bz2")),
        ("http://s3.amazonaws.com/agile_data_science/origin_dest_distances.jsonl", 
         os.path.join(data_path, "origin_dest_distances.jsonl")),
        
        # Modelos en la carpeta models/
        ("http://s3.amazonaws.com/agile_data_science/sklearn_vectorizer.pkl", 
         os.path.join(models_path, "sklearn_vectorizer.pkl")),
        ("http://s3.amazonaws.com/agile_data_science/sklearn_regressor.pkl", 
         os.path.join(models_path, "sklearn_regressor.pkl"))
    ]

    for url, dest in resources:
        download_file(url, dest)

if __name__ == "__main__":
    main()
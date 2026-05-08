import os
import urllib.request
import logging
import boto3
from botocore.config import Config

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

S3_CLIENT = boto3.client(
    's3',
    endpoint_url=os.getenv('MINIO_LOCAL_ENDPOINT', 'http://localhost:9000'),
    aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'admin'),
    aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'password'),
    use_ssl=False,
    config=Config(signature_version='s3v4')
)

BUCKET = os.getenv('ICEBERG_CATALOG', 'lakehouse')

def upload_to_minio(local_path, s3_key):
    try:
        S3_CLIENT.upload_file(local_path, BUCKET, s3_key)
        logging.info(f"✅ Subido a MinIO: s3://{BUCKET}/{s3_key}")
    except Exception as e:
        logging.error(f"❌ Error subiendo {s3_key} a MinIO: {e}")

def download_file(url, dest_path, s3_key=None):
    if os.path.exists(dest_path):
        logging.info(f"✔ El archivo ya existe: {dest_path}")
    else:
        logging.info(f"⏳ Descargando {url}...")
        try:
            urllib.request.urlretrieve(url, dest_path)
            logging.info(f"✅ Guardado en {dest_path}")
        except Exception as e:
            logging.error(f"❌ Error descargando {url}: {e}")
            return

    if s3_key:
        upload_to_minio(dest_path, s3_key)

def main():
    base_path = os.path.expanduser("~/ibdn")
    data_path = os.path.join(base_path, "data")
    models_path = os.path.join(base_path, "models")

    os.makedirs(data_path, exist_ok=True)
    os.makedirs(models_path, exist_ok=True)

    resources = [
        (os.getenv('FLIGHT_DELAYS_URL', 'http://s3.amazonaws.com/agile_data_science/simple_flight_delay_features.jsonl.bz2'), 
         os.path.join(data_path, "simple_flight_delay_features.jsonl.bz2"),
         "raw/simple_flight_delay_features.jsonl.bz2"),
        (os.getenv('ORIGIN_DEST_DISTANCES_URL', 'http://s3.amazonaws.com/agile_data_science/origin_dest_distances.jsonl'), 
         os.path.join(data_path, "origin_dest_distances.jsonl"),
         "raw/origin_dest_distances.jsonl"),
        (os.getenv('VECTORIZER_URL', 'http://s3.amazonaws.com/agile_data_science/sklearn_vectorizer.pkl'), 
         os.path.join(models_path, "sklearn_vectorizer.pkl"),
         "models/sklearn_vectorizer.pkl"),
        (os.getenv('REGRESSOR_URL', 'http://s3.amazonaws.com/agile_data_science/sklearn_regressor.pkl'), 
         os.path.join(models_path, "sklearn_regressor.pkl"),
         "models/sklearn_regressor.pkl")
    ]

    for url, dest, s3_key in resources:
        download_file(url, dest, s3_key)

if __name__ == "__main__":
    main()
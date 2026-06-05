import os, time
import boto3
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def _wait_for_minio(fn, retries=12, delay=5):
    for attempt in range(retries):
        try:
            return fn()
        except (EndpointConnectionError, ConnectionClosedError) as e:
            if attempt < retries - 1:
                logging.warning(f"MinIO no disponible (intento {attempt+1}/{retries}): {e}")
                time.sleep(delay)
            else:
                raise

def main():
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_LOCAL_ENDPOINT', 'http://localhost:9000'),
        aws_access_key_id=os.getenv('MINIO_ROOT_USER', 'admin'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD', 'password'),
        use_ssl=False,
        config=Config(signature_version='s3v4')
    )

    bucket_name = os.getenv('ICEBERG_CATALOG', 'lakehouse')
    buckets = _wait_for_minio(lambda: [b['Name'] for b in s3.list_buckets()['Buckets']])
    if bucket_name not in buckets:
        s3.create_bucket(Bucket=bucket_name)
        logging.info(f"✅ Bucket '{bucket_name}' creado en MinIO")
    else:
        logging.info(f"ℹ️  Bucket '{bucket_name}' ya existe")

if __name__ == "__main__":
    main()

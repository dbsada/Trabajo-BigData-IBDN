import os
import boto3
from botocore.config import Config
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

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
    buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if bucket_name not in buckets:
        s3.create_bucket(Bucket=bucket_name)
        logging.info(f"✅ Bucket '{bucket_name}' creado en MinIO")
    else:
        logging.info(f"ℹ️  Bucket '{bucket_name}' ya existe")

if __name__ == "__main__":
    main()

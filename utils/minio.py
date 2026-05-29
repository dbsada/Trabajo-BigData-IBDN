import os
import boto3
from botocore.config import Config

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password"),
        config=Config(signature_version="s3v4"),
    )
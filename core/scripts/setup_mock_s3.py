import boto3
import os
from pathlib import Path

# Config for LocalStack
ENDPOINT_URL = 'http://localhost:4566'
BUCKET_NAME = 'mock-bioprocess-bucket'
OBJECT_KEY = 'test_bioreactor_data.jsonl'

# Use pathlib to compute file path relative to script location
SCRIPT_DIR = Path(__file__).parent
SAMPLE_FILE_PATH = SCRIPT_DIR / "../tests/test_bioreactor_data.jsonl"

# Dummy creds for LocalStack
s3 = boto3.client(
    's3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

def create_bucket():
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' created successfully.")
    except s3.exceptions.BucketAlreadyExists:
        print(f"Bucket '{BUCKET_NAME}' already exists.")
    except Exception as e:
        print(f"Error creating bucket: {e}")

def upload_sample_data():
    if not SAMPLE_FILE_PATH.exists():
        raise FileNotFoundError(f"Sample file not found: {SAMPLE_FILE_PATH}")
    try:
        s3.upload_file(str(SAMPLE_FILE_PATH), BUCKET_NAME, OBJECT_KEY)
        print(f"Uploaded '{SAMPLE_FILE_PATH}' to '{BUCKET_NAME}/{OBJECT_KEY}'.")
    except Exception as e:
        print(f"Error uploading file: {e}")

def list_bucket_contents():
    response = s3.list_objects_v2(Bucket=BUCKET_NAME)
    if 'Contents' in response:
        for obj in response['Contents']:
            print(f"Object: {obj['Key']}")
    else:
        print("Bucket is empty.")

if __name__ == "__main__":
    create_bucket()
    upload_sample_data()
    list_bucket_contents()
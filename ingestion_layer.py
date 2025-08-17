import json
import os
from typing import Protocol, Iterable, List, Dict, Any
import boto3
import requests
from botocore.exceptions import ClientError

class IngestionLayer(Protocol):
    """Protocol for data ingestion layers, yielding chunks of readings."""

    def __init__(self, chunk_size: int = 1000):
        """
        Args:
            chunk_size: the number of readings per chunk
        """
        self.chunk_size = chunk_size

class FileIngestionLayer:
    """Ingestion layer for reading from a file."""

    def __init__(self, chunk_size: int = 1000, encoding: str = 'utf-8'):
        """
        Args:
            chunk_size: the number of readings per chunk
        """
        self.chunk_size = chunk_size
        self.encoding = encoding

    def ingest(self, source: str) -> Iterable[List[Dict[str, Any]]]:
        """yields chunks of readings from the source (eg. file path or API URL).

        Args:
            source: the data source (eg. file path or API URL)

        Returns:
            Chunks as lists of dicts (eg., 1000 readings per chunk)
        """

        chunk: List[Dict[str, Any]] = []

        with open(source, 'r', encoding=self.encoding) as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue # skip empty lines
                try: 
                    reading = json.loads(line)
                    chunk.append(reading)
                    if len(chunk) >= self.chunk_size:
                        yield chunk
                        chunk = []
                except json.JSONDecodeError:
                    print(f"Error parsing JSON: {line}")
                    continue
        if chunk:
            yield chunk


class APIIngestionLayer:
    """Ingestion layer for reading from an API."""

    def __init__(self, chunk_size: int = 1000, timeout: int = 30):
        """
        Args:
            chunk_size: the number of readings per chunk
        """
        self.chunk_size = chunk_size
        self.timeout = timeout
        
    def ingest(self, source: str) -> Iterable[List[Dict[str, Any]]]:
        """ Ingest from REST API with pagination. """
        chunk = []
        page = 1

        while True:
            response = requests.get(f"{source}?page={page}", timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            
            # Add items one by one to respect chunk_size
            for item in data:
                chunk.append(item)
                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = []
            
            page += 1
        if chunk:
            yield chunk


class S3IngestionLayer:

    def __init__(self, bucket_name: str, object_key: str, chunk_size: int = 1000, endpoint_url: str = None):
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.chunk_size = chunk_size
        self.endpoint_url = endpoint_url

    def ingest(self) -> Iterable[List[Dict[str, Any]]]:
        """
            Ingest data from an S3 bucket (or LocalStack simulation).
            Yields chunks of JSONL data for processing.
            
            Args:
                bucket_name: Name of the S3 bucket.
                object_key: Key (path) to the object in the bucket.
                chunk_size: Number of lines per chunk.
                endpoint_url: Optional custom endpoint (e.g., for LocalStack: 'http://localhost:4566').
        """
        # Use env vars for auth (fallback to defaults for LocalStack)
        access_key = os.getenv('AWS_ACCESS_KEY_ID', 'test')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'test')
        region = os.getenv('AWS_REGION', 'us-east-1')  # Default region

        s3 = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            endpoint_url=self.endpoint_url  # None for real AWS
        )

        try:
            response = s3.get_object(Bucket=self.bucket_name, Key=self.object_key)
            data = response['Body'].read().decode('utf-8').splitlines()
            for i in range(0, len(data), self.chunk_size):
                chunk = []
                for line in data[i:i + self.chunk_size]:
                    try:
                        chunk.append(json.loads(line))
                    except json.JSONDecodeError:
                        print(f"Skipping invalid JSON line: {line}")
                if chunk:  # Only yield non-empty chunks
                    yield chunk
        except ClientError as e:
            raise ValueError(f"Error fetching from S3: {e}")
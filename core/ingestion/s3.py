"""S3-based ingestion layer implementation (works with AWS or LocalStack)."""

import json
import os
from typing import Iterable, List, Dict, Any

import boto3
from botocore.exceptions import ClientError


class S3IngestionLayer:
    """Downloads a JSONL object from S3 and yields its content in chunks."""

    def __init__(
        self,
        bucket_name: str,
        object_key: str,
        chunk_size: int = 1000,
        endpoint_url: str | None = None,
    ):
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.chunk_size = chunk_size
        self.endpoint_url = endpoint_url

    def ingest(self) -> Iterable[List[Dict[str, Any]]]:
        """Yield parsed data in chunks from the configured S3 object."""
        # Credentials fallback for LocalStack usage
        access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
        region = os.getenv("AWS_REGION", "us-east-1")

        s3 = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            endpoint_url=self.endpoint_url,
        )

        try:
            response = s3.get_object(Bucket=self.bucket_name, Key=self.object_key)
            data = response["Body"].read().decode("utf-8").splitlines()
            for i in range(0, len(data), self.chunk_size):
                chunk: List[Dict[str, Any]] = []
                for line in data[i : i + self.chunk_size]:
                    try:
                        chunk.append(json.loads(line))
                    except json.JSONDecodeError:
                        print(f"Skipping invalid JSON line: {line}")
                if chunk:
                    yield chunk
        except ClientError as e:
            raise ValueError(f"Error fetching from S3: {e}")

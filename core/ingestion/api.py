"""HTTP-API–based ingestion layer implementation."""

from typing import Iterable, List, Dict, Any
import requests


class APIIngestionLayer:
    """Streams paginated JSON data from a REST API endpoint."""

    def __init__(self, chunk_size: int = 1000, timeout: int = 30):
        self.chunk_size = chunk_size
        self.timeout = timeout

    def ingest(self, source: str) -> Iterable[List[Dict[str, Any]]]:
        """Yield data from paginated HTTP GET requests as fixed-size chunks."""
        chunk: List[Dict[str, Any]] = []
        page = 1
        while True:
            response = requests.get(f"{source}?page={page}", timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            if not data:
                break

            for item in data:
                chunk.append(item)
                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = []
            page += 1

        if chunk:
            yield chunk

import json
from typing import Protocol, Iterable, List, Dict, Any
import requests

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
import json
from typing import Protocol, Iterable, List, Dict, Any

class IngestionLayer(Protocol):
    """Protocol for data ingestion layers, yielding chunks of readings."""

    def __init__(self, chunk_size: int = 1000):
        """
        Args:
            chunk_size: the number of readings per chunk
        """
        self.chunk_size = chunk_size


    def ingest(self, source: str) -> Iterable[List[Dict[str, Any]]]:
        """yields chunks of readings from the source (eg. file path or API URL).

        Args:
            source: the data source (eg. file path or API URL)

        Returns:
            Chunks as lists of dicts (eg., 1000 readings per chunk)
        """

        chunk: List[Dict[str, Any]] = []

        with open(source, 'r', encoding='utf-8') as file:
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
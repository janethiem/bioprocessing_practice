"""File-based ingestion layer implementation."""

import json
from typing import Iterable, List, Dict, Any


class FileIngestionLayer:
    """Reads newline-delimited JSON (JSONL) files and yields data in fixed-size chunks."""

    def __init__(self, chunk_size: int = 1000, encoding: str = "utf-8"):
        self.chunk_size = chunk_size
        self.encoding = encoding

    def ingest(self, source: str) -> Iterable[List[Dict[str, Any]]]:
        """Yield chunks of parsed JSON objects from *source*."""
        chunk: List[Dict[str, Any]] = []
        with open(source, "r", encoding=self.encoding) as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue  # Skip blank lines
                try:
                    reading = json.loads(line)
                except json.JSONDecodeError:
                    # Skip malformed JSON but continue processing.
                    print(f"Error parsing JSON: {line}")
                    continue
                chunk.append(reading)
                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = []
        if chunk:
            yield chunk

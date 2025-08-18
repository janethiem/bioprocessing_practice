"""
Core protocol for ingestion layers.
"""

from typing import Protocol, Iterable, List, Dict, Any

class IngestionLayer(Protocol):
    """Protocol for data ingestion layers that yield chunks of sensor readings."""

    chunk_size: int

    def __init__(self, chunk_size: int = 1000):
        """Initialise with the desired chunk size."""
        ...

    def ingest(self, source: str) -> Iterable[List[Dict[str, Any]]]:
        """Yield successive chunks of records taken from *source*."""
        ...

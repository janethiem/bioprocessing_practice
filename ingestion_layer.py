"""Backward-compatibility shim for legacy imports.

This module re-exports the new ingestion classes located under
`core.ingestion.*` so that existing code that still does
`import ingestion_layer` continues to work.

New code should import from `core.ingestion` directly::

    from core.ingestion.file import FileIngestionLayer
"""

from core.ingestion.base import IngestionLayer  # type: ignore F401
from core.ingestion.file import FileIngestionLayer  # type: ignore F401
from core.ingestion.api import APIIngestionLayer  # type: ignore F401
from core.ingestion.s3 import S3IngestionLayer  # type: ignore F401

__all__: list[str] = [
    "IngestionLayer",
    "FileIngestionLayer",
    "APIIngestionLayer",
    "S3IngestionLayer",
]
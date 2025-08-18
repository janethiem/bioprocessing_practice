"""Public API for the core package.

Importing `core` gives quick access to the main pipeline helpers
while still allowing power-users to reach into the sub-packages.
"""

from .pipeline import (
    process_pipeline,
    process_pipeline_streaming,
    process_data,
    IngestionLayerFactory,
)

__all__: list[str] = [
    "process_pipeline",
    "process_pipeline_streaming",
    "process_data",
    "IngestionLayerFactory",
]

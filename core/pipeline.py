"""
Bioprocessing module
Problem Setup: You're given a list of bioreactor sensor readings 
(as dicts, simulating API or file input).
Each has sensor_id (str), timestamp (str, e.g., "2025-08-16 14:30"), ph_value (float or invalid),
and temperature (float or invalid). Write a Python 3.11 function to:

Filter out invalid readings (non-numeric/negative ph_value or temperature, missing sensor_id or timestamp).
Group by sensor_id, computing average ph_value and counting anomalies (temperature > 40.0 or < 20.0).
Return a dict with sensor_id as keys and a tuple of (avg_ph, anomaly_count, latest_timestamp) as values.
"""

from dataclasses import dataclass
import os
from typing import Tuple
from urllib.parse import urlparse

from core.ingestion import IngestionLayer, APIIngestionLayer, FileIngestionLayer
from core.validation import SensorAggregator, ValidationLayer

@dataclass(frozen=True)
class SensorReading:
    sensor_id: str
    timestamp: str
    ph_value: float
    temperature: float

@dataclass
class SensorResult:
    avg_ph: float
    anomaly_count: int
    latest_timestamp: str

    def to_tuple(self) -> Tuple[float, int, str]:
        return (self.avg_ph, self.anomaly_count, self.latest_timestamp)

class IngestionLayerFactory:
    """Factory for creating appropriate ingestion layer based on source."""

    @staticmethod
    def create_ingestion_layer(source: str, chunk_size: int = 1000, **kwargs) -> IngestionLayer:
        """
        Create appropriate ingestion layer based on source.
        
        Args:
            source: Data source (file path, URL, etc.)
            chunk_size: Number of readings per chunk
            **kwargs: Additional parameters for specific layers
            
        Returns:
            Appropriate ingestion layer instance
            
        Raises:
            ValueError: If source type cannot be determined
        """

        # Check if the source is a URL (API source)
        parsed_url = urlparse(source)
        if parsed_url.scheme in ['http', 'https']:
            return APIIngestionLayer(chunk_size=chunk_size, timeout=kwargs.get('timeout', 30))

        # Check if it's a file path
        if os.path.isfile(source) or source.endswith(('.json', '.jsonl', '.txt', '.csv')):
            return FileIngestionLayer(
                chunk_size=chunk_size,
                encoding=kwargs.get('encoding', 'utf-8')
            )
        
        # Could add more detection logic here:
        # - Database connections (check for connection strings)
        # - S3 URLs (s3://)
        # - FTP URLs (ftp://)
        # - Streaming sources, etc.
        
        raise ValueError(f"Cannot determine ingestion layer for source: {source}")
        

def process_pipeline(source: str, chunk_size: int = 1000, **ingestion_kwargs):
    """
    Process data pipeline with efficient single-pass aggregation.
    
    Args:
        source: Data source (file path, URL, etc.)
        chunk_size: Number of readings per chunk
        **ingestion_kwargs: Additional parameters for ingestion layers
        
    Returns:
        Final aggregated sensor data as dict
    """
    ingestion_layer = IngestionLayerFactory.create_ingestion_layer(
        source, chunk_size, **ingestion_kwargs
    )
    aggregator = SensorAggregator()
    
    for chunk in ingestion_layer.ingest(source):
        aggregator.process_chunk(chunk)
    
    return aggregator.get_results()

            
# Legacy function kept for backward compatibility
def process_data(readings):
    """Legacy function - prefer using SensorAggregator for new code."""
    from collections import defaultdict
    from datetime import datetime
    
    sensor_data = defaultdict(lambda: {
        'ph_sum': 0,
        'ph_count': 0,
        'anomaly_count': 0,
        'latest_timestamp_str': None,
        'latest_timestamp_obj': None
    })
    
    for reading in readings:
        # Filter out invalid readings
        sensor_id = reading.get("sensor_id")
        ph_value = reading.get("ph_value")
        temperature = reading.get("temperature")
        timestamp = reading.get("timestamp")
        if not sensor_id or not timestamp or not ph_value or not temperature:
            continue
        if not isinstance(ph_value, (int, float)) or not isinstance(temperature, (int, float)):
            continue
        if ph_value < 0 or temperature < 0:
            continue

        # Update running totals
        sensor_data[sensor_id]['ph_sum'] += float(ph_value)
        sensor_data[sensor_id]['ph_count'] += 1

        # Count anomalies
        if temperature > 40.0 or temperature < 20.0:
            sensor_data[sensor_id]['anomaly_count'] += 1

        # Convert timestamp to datetime object
        try:
            timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
        except ValueError:
            continue

        # Track latest timestamps
        if not sensor_data[sensor_id]['latest_timestamp_obj'] or timestamp_obj > sensor_data[sensor_id]['latest_timestamp_obj']:
            sensor_data[sensor_id]['latest_timestamp_obj'] = timestamp_obj
            sensor_data[sensor_id]['latest_timestamp_str'] = timestamp

    # Compute averages and format results
    results = {}
    for sensor_id, data in sensor_data.items():
        avg_ph = data['ph_sum'] / data['ph_count'] if data['ph_count'] > 0 else 0
        results[sensor_id] = (avg_ph, data['anomaly_count'], data['latest_timestamp_str'])
    
    return results

# Keep the old streaming version for real-time use cases
def process_pipeline_streaming(source: str, chunk_size: int = 1000, **ingestion_kwargs):
    """
    Streaming version that yields partial results per chunk.
    Useful for real-time dashboards or when you need incremental updates.
    """
    ingestion_layer = IngestionLayerFactory.create_ingestion_layer(
        source, chunk_size, **ingestion_kwargs
    )
    validation_layer = ValidationLayer()
    
    for chunk in ingestion_layer.ingest(source):
        valid_chunk = validation_layer.filter_chunk(chunk)
        if valid_chunk:
            yield process_data(valid_chunk)
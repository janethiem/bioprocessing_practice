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
from datetime import datetime
from typing import List, Dict, Tuple
from collections import defaultdict

from ingestion_layer import IngestionLayer
from validation_layer import ValidationLayer

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



# pipeline.py
def process_pipeline(source: str, chunk_size: int = 1000):
    ingestion_layer = IngestionLayer(chunk_size=chunk_size)
    validation_layer = ValidationLayer()
    
    for chunk in ingestion_layer.ingest(source):
        valid_chunk = validation_layer.filter_chunk(chunk)
        if valid_chunk:
            # Process and yield immediately
            yield process_data(valid_chunk)
            
def process_data(readings: List[SensorReading]) -> Dict[str, Tuple[float, int, str]]:
    sensor_data = defaultdict(lambda: {
        'ph_sum': 0,
        'ph_count': 0,
        'anomaly_count': 0,
        'latest_timestamp_str': None,
        'latest_timestamp_obj': None
    })
    for reading in readings:
        # Filter out invalid readings (non-numeric/negative ph_value or temperature, missing sensor_id or timestamp).
        sensor_id = reading.get("sensor_id")
        ph_value = reading.get("ph_value")
        temperature = reading.get("temperature")
        timestamp = reading.get("timestamp")
        if not sensor_id or not timestamp or not ph_value or not temperature:
            continue
        if not isinstance(ph_value, float) or not isinstance(temperature, float):
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
        results[sensor_id] = SensorResult(avg_ph=avg_ph, anomaly_count=data['anomaly_count'], latest_timestamp=data['latest_timestamp_str']).to_tuple()
    
    return results
        

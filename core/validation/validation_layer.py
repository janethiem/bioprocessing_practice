# validation_layer.py
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List


class ValidationLayer:
    def __init__(self):
        self.required_fields = ['sensor_id', 'timestamp', 'ph_value', 'temperature']
    
    def validate_reading(self, reading: Dict[str, Any]) -> bool:
        sensor_id = reading.get("sensor_id")
        ph_value = reading.get("ph_value")
        temperature = reading.get("temperature")
        timestamp = reading.get("timestamp")
        
        if not sensor_id or not timestamp or ph_value is None or temperature is None:
            return False
        if not isinstance(ph_value, (int, float)) or not isinstance(temperature, (int, float)):
            return False
        if ph_value < 0 or temperature < 0:
            return False
        return True
    
    def filter_chunk(self, chunk: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [reading for reading in chunk if self.validate_reading(reading)]

class SensorAggregator:
    """Handles validation and aggregation in a single pass for efficiency."""
    
    def __init__(self):
        self.validator = ValidationLayer()
        self.sensor_data = defaultdict(lambda: {
            'ph_sum': 0.0,
            'ph_count': 0,
            'anomaly_count': 0,
            'latest_timestamp_str': None,
            'latest_timestamp_obj': None
        })
    
    def process_chunk(self, chunk: List[Dict[str, Any]]) -> None:
        """Process a chunk of readings, updating running aggregates."""
        for reading in chunk:
            if not self.validator.validate_reading(reading):
                continue
                
            sensor_id = reading["sensor_id"]
            ph_value = float(reading["ph_value"])
            temperature = float(reading["temperature"])
            timestamp = reading["timestamp"]
            
            # Validate timestamp format early - if invalid, skip entire reading
            try:
                timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M")
            except ValueError:
                continue  # Skip readings with invalid timestamps
            
            # Update running totals
            self.sensor_data[sensor_id]['ph_sum'] += ph_value
            self.sensor_data[sensor_id]['ph_count'] += 1
            
            # Count temperature anomalies
            if temperature > 40.0 or temperature < 20.0:
                self.sensor_data[sensor_id]['anomaly_count'] += 1
            
            # Track latest timestamp
            if (not self.sensor_data[sensor_id]['latest_timestamp_obj'] or 
                timestamp_obj > self.sensor_data[sensor_id]['latest_timestamp_obj']):
                self.sensor_data[sensor_id]['latest_timestamp_obj'] = timestamp_obj
                self.sensor_data[sensor_id]['latest_timestamp_str'] = timestamp
    
    def get_results(self) -> Dict[str, tuple]:
        """Get final aggregated results."""
        results = {}
        for sensor_id, data in self.sensor_data.items():
            avg_ph = data['ph_sum'] / data['ph_count'] if data['ph_count'] > 0 else 0.0
            results[sensor_id] = (avg_ph, data['anomaly_count'], data['latest_timestamp_str'])
        return results
    
    def reset(self) -> None:
        """Reset aggregator for reuse."""
        self.sensor_data.clear()
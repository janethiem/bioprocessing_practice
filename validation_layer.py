# validation_layer.py
from typing import Any, Dict, List


class ValidationLayer:
    def __init__(self):
        self.required_fields = ['sensor_id', 'timestamp', 'ph_value', 'temperature']
    
    def validate_reading(self, reading: Dict[str, Any]) -> bool:
        # Your validation logic from bioprocessing.py
        sensor_id = reading.get("sensor_id")
        ph_value = reading.get("ph_value")
        temperature = reading.get("temperature")
        timestamp = reading.get("timestamp")
        
        if not sensor_id or not timestamp or not ph_value or not temperature:
            return False
        if not isinstance(ph_value, (int, float)) or not isinstance(temperature, (int, float)):
            return False
        if ph_value < 0 or temperature < 0:
            return False
        return True
    
    def filter_chunk(self, chunk: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [reading for reading in chunk if self.validate_reading(reading)]
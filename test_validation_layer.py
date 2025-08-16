# test_validation_layer.py
from validation_layer import ValidationLayer

def test_validation_layer():
    """Test that validation layer filters invalid data."""
    validation_layer = ValidationLayer()
    raw_chunk = [
        {"sensor_id": "BioR1", "ph_value": 7.2, "temperature": 37.5, "timestamp": "2025-08-16 14:00"},
        {"sensor_id": "", "ph_value": 7.4, "temperature": 38.0, "timestamp": "2025-08-16 14:00"},  # Invalid
        {"sensor_id": "BioR2", "ph_value": -1.0, "temperature": 25.0, "timestamp": "2025-08-16 14:00"}  # Invalid
    ]
    
    valid_chunk = validation_layer.filter_chunk(raw_chunk)
    assert len(valid_chunk) == 1  # Only first reading is valid
    assert valid_chunk[0]["sensor_id"] == "BioR1"
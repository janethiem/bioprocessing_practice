# test_validation_layer.py
import sys
from pathlib import Path

# Add parent directory to path so we can import core modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.validation.validation_layer import ValidationLayer, SensorAggregator

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

def test_validation_individual_readings():
    """Test individual reading validation logic."""
    validator = ValidationLayer()
    
    # Valid reading
    valid_reading = {"sensor_id": "BioR1", "ph_value": 7.2, "temperature": 37.5, "timestamp": "2025-08-16 14:00"}
    assert validator.validate_reading(valid_reading) == True
    
    # Invalid readings
    assert validator.validate_reading({"sensor_id": "", "ph_value": 7.2, "temperature": 37.5, "timestamp": "2025-08-16 14:00"}) == False  # Empty sensor_id
    assert validator.validate_reading({"sensor_id": "BioR1", "ph_value": -1.0, "temperature": 37.5, "timestamp": "2025-08-16 14:00"}) == False  # Negative pH
    assert validator.validate_reading({"sensor_id": "BioR1", "ph_value": 7.2, "temperature": -5.0, "timestamp": "2025-08-16 14:00"}) == False  # Negative temperature
    assert validator.validate_reading({"sensor_id": "BioR1", "ph_value": "invalid", "temperature": 37.5, "timestamp": "2025-08-16 14:00"}) == False  # Non-numeric pH
    assert validator.validate_reading({"sensor_id": "BioR1", "ph_value": 7.2, "temperature": 37.5, "timestamp": ""}) == False  # Empty timestamp
    assert validator.validate_reading({"ph_value": 7.2, "temperature": 37.5, "timestamp": "2025-08-16 14:00"}) == False  # Missing sensor_id

def test_sensor_aggregator_basic():
    """Test basic SensorAggregator functionality."""
    aggregator = SensorAggregator()
    
    chunk = [
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:00", "ph_value": 7.0, "temperature": 35.0},
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:30", "ph_value": 8.0, "temperature": 25.0},
        {"sensor_id": "BioR2", "timestamp": "2025-08-16 14:00", "ph_value": 6.5, "temperature": 45.0},  # High temp anomaly
    ]
    
    aggregator.process_chunk(chunk)
    results = aggregator.get_results()
    
    # Check BioR1 results
    assert "BioR1" in results
    avg_ph, anomaly_count, latest_timestamp = results["BioR1"]
    assert avg_ph == 7.5  # (7.0 + 8.0) / 2
    assert anomaly_count == 0  # No temperature anomalies
    assert latest_timestamp == "2025-08-16 14:30"
    
    # Check BioR2 results
    assert "BioR2" in results
    avg_ph, anomaly_count, latest_timestamp = results["BioR2"]
    assert avg_ph == 6.5
    assert anomaly_count == 1  # One high temperature anomaly
    assert latest_timestamp == "2025-08-16 14:00"

def test_sensor_aggregator_temperature_anomalies():
    """Test temperature anomaly detection."""
    aggregator = SensorAggregator()
    
    chunk = [
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:00", "ph_value": 7.0, "temperature": 45.0},  # High temp
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:30", "ph_value": 7.0, "temperature": 15.0},  # Low temp
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 15:00", "ph_value": 7.0, "temperature": 30.0},  # Normal temp
    ]
    
    aggregator.process_chunk(chunk)
    results = aggregator.get_results()
    
    avg_ph, anomaly_count, latest_timestamp = results["BioR1"]
    assert avg_ph == 7.0
    assert anomaly_count == 2  # Two anomalies: >40.0 and <20.0
    assert latest_timestamp == "2025-08-16 15:00"

def test_sensor_aggregator_invalid_data_filtering():
    """Test that aggregator filters out invalid data."""
    aggregator = SensorAggregator()
    
    chunk = [
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:00", "ph_value": 7.0, "temperature": 35.0},  # Valid
        {"sensor_id": "", "timestamp": "2025-08-16 14:30", "ph_value": 8.0, "temperature": 25.0},  # Invalid: empty sensor_id
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 15:00", "ph_value": -1.0, "temperature": 30.0},  # Invalid: negative pH
        {"sensor_id": "BioR1", "timestamp": "invalid-timestamp", "ph_value": 7.5, "temperature": 32.0},  # Invalid timestamp format
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 16:00", "ph_value": 7.5, "temperature": 32.0},  # Valid
    ]
    
    aggregator.process_chunk(chunk)
    results = aggregator.get_results()
    
    # Should only process the 2 valid readings
    avg_ph, anomaly_count, latest_timestamp = results["BioR1"]
    assert avg_ph == 7.25  # (7.0 + 7.5) / 2
    assert anomaly_count == 0
    assert latest_timestamp == "2025-08-16 16:00"

def test_sensor_aggregator_multiple_chunks():
    """Test aggregator accumulates data across multiple chunks."""
    aggregator = SensorAggregator()
    
    chunk1 = [
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:00", "ph_value": 6.0, "temperature": 35.0},
    ]
    chunk2 = [
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:30", "ph_value": 8.0, "temperature": 45.0},  # Anomaly
        {"sensor_id": "BioR2", "timestamp": "2025-08-16 14:00", "ph_value": 7.0, "temperature": 30.0},
    ]
    chunk3 = [
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 15:00", "ph_value": 7.0, "temperature": 25.0},
    ]
    
    aggregator.process_chunk(chunk1)
    aggregator.process_chunk(chunk2)
    aggregator.process_chunk(chunk3)
    
    results = aggregator.get_results()
    
    # BioR1: 3 readings, avg pH = (6.0 + 8.0 + 7.0) / 3 = 7.0, 1 anomaly
    avg_ph, anomaly_count, latest_timestamp = results["BioR1"]
    assert avg_ph == 7.0
    assert anomaly_count == 1
    assert latest_timestamp == "2025-08-16 15:00"
    
    # BioR2: 1 reading
    avg_ph, anomaly_count, latest_timestamp = results["BioR2"]
    assert avg_ph == 7.0
    assert anomaly_count == 0
    assert latest_timestamp == "2025-08-16 14:00"

def test_sensor_aggregator_reset():
    """Test aggregator reset functionality."""
    aggregator = SensorAggregator()
    
    chunk = [
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:00", "ph_value": 7.0, "temperature": 35.0},
    ]
    
    aggregator.process_chunk(chunk)
    assert len(aggregator.get_results()) == 1
    
    aggregator.reset()
    assert len(aggregator.get_results()) == 0
    
    # Should be able to use aggregator again after reset
    aggregator.process_chunk(chunk)
    results = aggregator.get_results()
    assert len(results) == 1
    assert "BioR1" in results

def test_sensor_aggregator_edge_cases():
    """Test edge cases for aggregator."""
    aggregator = SensorAggregator()
    
    # Empty chunk
    aggregator.process_chunk([])
    assert len(aggregator.get_results()) == 0
    
    # Chunk with all invalid data
    invalid_chunk = [
        {"sensor_id": "", "timestamp": "2025-08-16 14:00", "ph_value": 7.0, "temperature": 35.0},
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:00", "ph_value": -1.0, "temperature": 35.0},
    ]
    aggregator.process_chunk(invalid_chunk)
    assert len(aggregator.get_results()) == 0
    
    # Single valid reading
    valid_chunk = [
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:00", "ph_value": 7.0, "temperature": 35.0},
    ]
    aggregator.process_chunk(valid_chunk)
    results = aggregator.get_results()
    assert len(results) == 1
    avg_ph, anomaly_count, _ = results["BioR1"]
    assert avg_ph == 7.0
    assert anomaly_count == 0
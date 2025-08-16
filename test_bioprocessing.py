from bioprocessing import process_data

MOCK_INPUT_READINGS = [
    {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:00", "ph_value": 7.2, "temperature": 37.5},
    {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:30", "ph_value": 7.1, "temperature": 45.0},  # Anomaly: high temp
    {"sensor_id": "BioR2", "timestamp": "2025-08-16 14:00", "ph_value": -1.0, "temperature": 25.0},  # Invalid: negative ph
    {"sensor_id": "BioR3", "timestamp": "2025-08-16 14:00", "ph_value": "invalid", "temperature": 30.0},  # Invalid: non-numeric ph
    {"sensor_id": "", "timestamp": "2025-08-16 14:00", "ph_value": 7.0, "temperature": 35.0},  # Invalid: empty sensor_id
]

MOCK_EXPECTED_OUTPUT = {"BioR1": (7.15, 1, "2025-08-16 14:30")}  # Avg ph 7.15, 1 anomaly, latest ts

def test_process_data():
    assert process_data(MOCK_INPUT_READINGS) == MOCK_EXPECTED_OUTPUT

# Command to run the test: python -m pytest test_bioprocessing.py
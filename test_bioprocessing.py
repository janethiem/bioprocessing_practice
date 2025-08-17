from bioprocessing import process_data, process_pipeline
from unittest.mock import patch

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

# Command to run the test: python -m pytest test_bioprocessing.py -v

def test_process_pipeline():
    pipeline = process_pipeline("test_bioreactor_data.jsonl")
    results = next(pipeline)
    
    # Test that we get results for BioR1 and BioR2
    assert "BioR1" in results
    assert "BioR2" in results
    
    # Test BioR1 has the expected structure
    bio1_result = results["BioR1"]
    assert len(bio1_result) == 3  # (avg_ph, anomaly_count, latest_timestamp)
    assert isinstance(bio1_result[0], float)  # avg_ph
    assert isinstance(bio1_result[1], int)    # anomaly_count
    assert isinstance(bio1_result[2], str)    # latest_timestamp


def test_process_pipeline_file_auto_detection():
    """The factory should pick FileIngestionLayer for a local path."""
    with patch('bioprocessing.FileIngestionLayer.ingest') as mock_ingest:
        # Mock one chunk yielded from the file layer
        mock_ingest.return_value = iter([MOCK_INPUT_READINGS])
        results = next(process_pipeline("test_bioreactor_data.jsonl"))
        mock_ingest.assert_called_once()          # File layer really used
        assert results == MOCK_EXPECTED_OUTPUT     # Data still processed correctly


def test_process_pipeline_api_auto_detection():
    """The factory should pick APIIngestionLayer for an HTTP URL."""
    with patch('bioprocessing.APIIngestionLayer.ingest') as mock_ingest:
        mock_ingest.return_value = iter([MOCK_INPUT_READINGS])
        results = next(process_pipeline("https://api.example.com/sensors"))
        mock_ingest.assert_called_once()          # API layer really used
        assert results == MOCK_EXPECTED_OUTPUT     # Data still processed correctly
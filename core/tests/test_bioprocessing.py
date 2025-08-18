import sys
from pathlib import Path

# Add parent directory to path so we can import core modules
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.pipeline import process_data, process_pipeline, process_pipeline_streaming
from core.validation import SensorAggregator
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
    """Test legacy process_data function."""
    assert process_data(MOCK_INPUT_READINGS) == MOCK_EXPECTED_OUTPUT

def test_sensor_aggregator():
    """Test the new SensorAggregator class."""
    aggregator = SensorAggregator()
    aggregator.process_chunk(MOCK_INPUT_READINGS)
    results = aggregator.get_results()
    assert results == MOCK_EXPECTED_OUTPUT

def test_sensor_aggregator_multiple_chunks():
    """Test aggregator handles multiple chunks correctly."""
    chunk1 = [
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:00", "ph_value": 7.0, "temperature": 35.0},
    ]
    chunk2 = [
        {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:30", "ph_value": 8.0, "temperature": 25.0},
    ]
    
    aggregator = SensorAggregator()
    aggregator.process_chunk(chunk1)
    aggregator.process_chunk(chunk2)
    results = aggregator.get_results()
    
    # Should have BioR1 with avg pH of 7.5, 0 anomalies, latest timestamp 14:30
    assert "BioR1" in results
    avg_ph, anomaly_count, latest_timestamp = results["BioR1"]
    assert avg_ph == 7.5
    assert anomaly_count == 0
    assert latest_timestamp == "2025-08-16 14:30"

def test_sensor_aggregator_reset():
    """Test aggregator reset functionality."""
    aggregator = SensorAggregator()
    aggregator.process_chunk(MOCK_INPUT_READINGS)
    assert len(aggregator.get_results()) > 0
    
    aggregator.reset()
    assert len(aggregator.get_results()) == 0

# Command to run the test: python -m pytest test_bioprocessing.py -v

def test_process_pipeline():
    """Test the new aggregated pipeline - returns final results directly."""
    # Use pathlib to get test data file path relative to this test script
    test_data_path = Path(__file__).parent / "test_bioreactor_data.jsonl"
    results = process_pipeline(str(test_data_path))
    
    # Test that we get results for BioR1 and BioR2
    assert "BioR1" in results
    assert "BioR2" in results
    
    # Test BioR1 has the expected structure
    bio1_result = results["BioR1"]
    assert len(bio1_result) == 3  # (avg_ph, anomaly_count, latest_timestamp)
    assert isinstance(bio1_result[0], float)  # avg_ph
    assert isinstance(bio1_result[1], int)    # anomaly_count
    assert isinstance(bio1_result[2], str)    # latest_timestamp

def test_process_pipeline_streaming():
    """Test streaming version still works and yields per-chunk results."""
    test_data_path = Path(__file__).parent / "test_bioreactor_data.jsonl"
    pipeline = process_pipeline_streaming(str(test_data_path))
    results = next(pipeline)  # Get first chunk results
    
    # Should still get results with the same structure
    assert isinstance(results, dict)
    if "BioR1" in results:
        bio1_result = results["BioR1"]
        assert len(bio1_result) == 3


def test_process_pipeline_file_auto_detection():
    """The factory should pick FileIngestionLayer for a local path."""
    with patch('core.ingestion.file.FileIngestionLayer.ingest') as mock_ingest:
        # Mock one chunk yielded from the file layer
        mock_ingest.return_value = iter([MOCK_INPUT_READINGS])
        test_data_path = Path(__file__).parent / "test_bioreactor_data.jsonl"
        results = process_pipeline(str(test_data_path))
        mock_ingest.assert_called_once()          # File layer really used
        assert results == MOCK_EXPECTED_OUTPUT     # Data still processed correctly


def test_process_pipeline_api_auto_detection():
    """The factory should pick APIIngestionLayer for an HTTP URL."""
    with patch('core.ingestion.api.APIIngestionLayer.ingest') as mock_ingest:
        mock_ingest.return_value = iter([MOCK_INPUT_READINGS])
        results = process_pipeline("https://api.example.com/sensors")
        mock_ingest.assert_called_once()          # API layer really used
        assert results == MOCK_EXPECTED_OUTPUT     # Data still processed correctly

def test_streaming_vs_aggregated_consistency():
    """Test that streaming and aggregated versions produce same final results."""
    with patch('core.ingestion.file.FileIngestionLayer.ingest') as mock_ingest:
        mock_ingest.return_value = iter([MOCK_INPUT_READINGS])
        
        # Test aggregated version
        test_data_path = Path(__file__).parent / "test_bioreactor_data.jsonl"
        aggregated_results = process_pipeline(str(test_data_path))
        
        # Reset mock for streaming test
        mock_ingest.reset_mock()
        mock_ingest.return_value = iter([MOCK_INPUT_READINGS])
        
        # Test streaming version (collect all chunks)
        streaming_results = {}
        for chunk_results in process_pipeline_streaming(str(test_data_path)):
            # In this simple test case, there's only one chunk, so results should be the same
            streaming_results = chunk_results
            break
        
        assert aggregated_results == streaming_results
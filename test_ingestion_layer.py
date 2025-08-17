import requests
from unittest.mock import patch, Mock
from ingestion_layer import FileIngestionLayer, APIIngestionLayer
import pytest

class TestFileIngestionLayer:
    def test_file_ingestion_layer(self):
        """Test that ingestion layer reads all data without filtering."""
        file_ingestion_layer = FileIngestionLayer(chunk_size=3)
        chunks = list(file_ingestion_layer.ingest("test_bioreactor_data.jsonl"))
    
        # Should get 4 chunks (10 lines / 3 per chunk = 4 chunks)
        assert len(chunks) == 4
        assert len(chunks[0]) == 3
        assert len(chunks[1]) == 3
        assert len(chunks[2]) == 3
        assert len(chunks[3]) == 1

class TestAPIIngestionLayer:
    
    @patch('ingestion_layer.requests.get')
    def test_successful_single_page_ingestion(self, mock_get):
        # Mock a single page response
        mock_response = Mock()
        mock_response.json.return_value = [
            {"sensor_id": "BioR1", "timestamp": "2025-08-16", "ph_value": 7.2, "temperature": 37.5},
            {"sensor_id": "BioR2", "timestamp": "2025-08-16", "ph_value": 7.1, "temperature": 36.0}
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.side_effect = [mock_response, Mock(json=Mock(return_value=[]))]
        
        layer = APIIngestionLayer(chunk_size=5)
        chunks = list(layer.ingest("http://test-api.com/data"))
        
        assert len(chunks) == 1
        assert len(chunks[0]) == 2
        mock_get.assert_called_with("http://test-api.com/data?page=2", timeout=30)

    @patch('ingestion_layer.requests.get')
    def test_pagination_with_chunking(self, mock_get):
        # Mock multiple pages that exceed chunk size
        page1_data = [{"id": i} for i in range(7)]  # 7 items
        page2_data = [{"id": i} for i in range(7, 10)]  # 3 items
        print("page1_data", page1_data)
        print("page2_data", page2_data)

        mock_responses = [
            Mock(json=Mock(return_value=page1_data)),
            Mock(json=Mock(return_value=page2_data)),
            Mock(json=Mock(return_value=[]))  # Empty page ends pagination
        ]
        for response in mock_responses:
            response.raise_for_status.return_value = None
        mock_get.side_effect = mock_responses
        
        layer = APIIngestionLayer(chunk_size=5)
        chunks = list(layer.ingest("http://test-api.com/data"))
        
        print("chunks", chunks)
        assert len(chunks) == 2
        assert len(chunks[0]) == 5  # First chunk: 5 items
        assert len(chunks[1]) == 5  # Second chunk: 2 from page1 + 3 from page2

    @patch('ingestion_layer.requests.get')
    def test_http_error_handling(self, mock_get):
        mock_get.side_effect = requests.HTTPError("404 Not Found")
        
        layer = APIIngestionLayer()
        
        with pytest.raises(requests.HTTPError):
            list(layer.ingest("http://test-api.com/nonexistent"))

    @patch('ingestion_layer.requests.get')
    def test_timeout_parameter(self, mock_get):
        layer = APIIngestionLayer(timeout=60)
        mock_get.return_value.json.return_value = []
        mock_get.return_value.raise_for_status.return_value = None
        
        list(layer.ingest("http://test-api.com/data"))
        
        mock_get.assert_called_with("http://test-api.com/data?page=1", timeout=60)
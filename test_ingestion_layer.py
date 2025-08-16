from ingestion_layer import IngestionLayer

def test_ingestion_layer():
    """Test that ingestion layer reads all data without filtering."""
    ingestion_layer = IngestionLayer(chunk_size=3)
    chunks = list(ingestion_layer.ingest("test_bioreactor_data.jsonl"))
    
    # Should get 4 chunks (10 lines / 3 per chunk = 4 chunks)
    assert len(chunks) == 4
    assert len(chunks[0]) == 3
    assert len(chunks[1]) == 3
    assert len(chunks[2]) == 3
    assert len(chunks[3]) == 1

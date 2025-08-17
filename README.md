# Bioprocessing Practice for InvertBio Interview

A modular data pipeline for processing bioreactor sensor readings with anomaly detection, validation, and flexible ingestion from files or APIs.

## Features

- **Multi-source ingestion**: Automatic detection and processing of file-based (JSON/JSONL) or API data sources
- **Data validation**: Filters invalid readings with configurable validation rules
- **Anomaly detection**: Identifies temperature anomalies (< 20°C or > 40°C)
- **Chunked processing**: Handles large datasets efficiently with configurable chunk sizes
- **Modular architecture**: Separate layers for ingestion, validation, and processing

## Setup

### Quick setup with script:
```bash
chmod +x setup.sh
./setup.sh
source venv/bin/activate
```

### Manual setup:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Basic processing
```python
from bioprocessing import process_data

readings = [
    {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:00", "ph_value": 7.2, "temperature": 37.5},
    {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:30", "ph_value": 7.1, "temperature": 45.0}
]

results = process_data(readings)
# Returns: {"BioR1": (7.15, 1, "2025-08-16 14:30")}
# Format: {sensor_id: (avg_ph, anomaly_count, latest_timestamp)}
```

### Pipeline processing (recommended)
```python
from bioprocessing import process_pipeline

# Process from file
for results in process_pipeline("data.jsonl"):
    print(results)

# Process from API
for results in process_pipeline("https://api.example.com/sensors"):
    print(results)

# Custom chunk size
for results in process_pipeline("data.jsonl", chunk_size=500):
    print(results)
```

## Data Format

Input readings must contain:
- `sensor_id` (string): Unique sensor identifier
- `timestamp` (string): Format "YYYY-MM-DD HH:MM"
- `ph_value` (float): pH measurement (must be ≥ 0)
- `temperature` (float): Temperature in °C (must be ≥ 0)

Invalid readings are automatically filtered out.

## Testing

### Run all tests:
```bash
pytest
```

### Run specific test suites:
```bash
# Core processing logic
pytest test_bioprocessing.py -v

# Ingestion layer tests
pytest test_ingestion_layer.py -v

# Validation layer tests  
pytest test_validation_layer.py -v
```

### Run with coverage:
```bash
pytest --cov=bioprocessing --cov=ingestion_layer --cov=validation_layer
```

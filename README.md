# Bioprocessing Practice for InvertBio Interview

A modular data pipeline for processing bioreactor sensor readings with anomaly detection, validation, and flexible ingestion from files or APIs.

## Architecture

The project is organized into a clean modular structure under the `core/` package:

- `core/ingestion/` - Data ingestion layers (file, API, S3)
- `core/validation/` - Data validation and aggregation logic  
- `core/pipeline.py` - Main pipeline orchestration
- `core/tests/` - Comprehensive test suite
- `core/scripts/` - Utility scripts for setup and testing

## Features

- **Multi-source ingestion**: Automatic detection and processing of file-based (JSON/JSONL), API, or S3 data sources
- **Data validation**: Filters invalid readings with configurable validation rules
- **Anomaly detection**: Identifies temperature anomalies (< 20°C or > 40°C)
- **Chunked processing**: Handles large datasets efficiently with configurable chunk sizes
- **S3 support**: Ingest data from S3 buckets with LocalStack support for local development
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

### S3 Development Setup (LocalStack)
For testing S3 functionality locally:
```bash
# Install and start LocalStack
pip install localstack
localstack start -d

# Set up mock S3 bucket and data
python core/scripts/setup_mock_s3.py
```

## Usage

### Basic processing
```python
from core import process_data

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
from core import process_pipeline

# Process from file
results = process_pipeline("data.jsonl")
print(results)

# Process from API
results = process_pipeline("https://api.example.com/sensors")
print(results)

# Custom chunk size
results = process_pipeline("data.jsonl", chunk_size=500)
print(results)
```

### S3 ingestion
```python
from core.ingestion import S3IngestionLayer

# For LocalStack (local development)
layer = S3IngestionLayer(
    bucket_name='my-bucket',
    object_key='data.jsonl',
    endpoint_url='http://localhost:4566'
)

# For AWS S3 (production)
layer = S3IngestionLayer(
    bucket_name='my-bucket', 
    object_key='data.jsonl'
)

for chunk in layer.ingest():
    print(f"Processing {len(chunk)} readings")
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
pytest core/tests/test_bioprocessing.py -v

# Ingestion layer tests
pytest core/tests/test_ingestion_layer.py -v

# Validation layer tests  
pytest core/tests/test_validation_layer.py -v

# S3 ingestion tests (requires LocalStack running)
python core/scripts/setup_mock_s3.py  # Set up test data first
pytest core/tests/test_ingestion_layer.py::TestS3IngestionLayer -v
```

### Run with coverage:
```bash
pytest --cov=core
```

## Future Enhancements

*Suggestions from the intelligent husband* 😊

### 🐳 Docker Composification & Database Integration

1. **Containerization Setup**
   - [ ] Create `docker-compose.yaml` file
   - [ ] Dockerize the bioprocessing application
   - [ ] Add LocalStack S3 as a Docker container in compose file

2. **Database Integration**
   - [ ] Add MySQL database container to docker-compose
   - [ ] Set up database schema for sensor readings
   - [ ] Implement database persistence layer

3. **Development Tools**
   - [ ] Download [Sequel Ace](https://sequel-ace.com/) (macOS GUI for MySQL visualization)
   - [ ] Configure database connection via docker-compose ports/credentials
   - [ ] Download [OrbStack](https://orbstack.dev/) (better Docker Desktop replacement for macOS)

### 📊 Monitoring & Observability

1. **Prometheus Integration**
   - [ ] Install and instrument Prometheus metrics in the application
   - [ ] Add custom metrics for processing pipeline performance
   - [ ] Track anomaly detection rates and processing times

2. **Infrastructure Monitoring**
   - [ ] Add Prometheus server container to docker-compose
   - [ ] Configure metric scraping endpoints
   - [ ] Set up service discovery for monitoring

3. **Visualization & Dashboards**
   - [ ] Add Grafana container to docker-compose
   - [ ] Configure Grafana to ingest Prometheus metrics
   - [ ] Build dashboards for pipeline performance and sensor data trends
   - [ ] Create test scenarios to generate meaningful metrics data for visualization
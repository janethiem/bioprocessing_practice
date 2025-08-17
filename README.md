# Bioprocessing Practice for InvertBio Interview

A modular data pipeline for processing bioreactor sensor readings with anomaly detection, validation, and flexible ingestion from files or APIs.

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
python setup_mock_s3.py
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

### S3 ingestion
```python
from ingestion_layer import S3IngestionLayer

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
pytest test_bioprocessing.py -v

# Ingestion layer tests
pytest test_ingestion_layer.py -v

# Validation layer tests  
pytest test_validation_layer.py -v

# S3 ingestion tests (requires LocalStack running)
python setup_mock_s3.py  # Set up test data first
pytest test_ingestion_layer.py::TestS3IngestionLayer -v
```

### Run with coverage:
```bash
pytest --cov=bioprocessing --cov=ingestion_layer --cov=validation_layer
```

### TODO's Suggested by Intelligent Husband
docker composification and add a database:
    1. docker compose yaml file
    2. dockerize your app
    3. make the localstack s3 thing a docker container in your compose file
    4. add mysql db as a container in docker compose
    5. download sequel ace (macos GUI for visualization mysql DBs), connect to your db via the port, username, db name, password in the docker compose file
    6. download orbstack (replacement for docker desktop that runs way better on macos), follow their install instructions to replace your existing docker desktop install


monitoring:
    1. add prometheus to your app (install it and instrument a few thing)
    2. add prometheus server as a container in docker compose yaml and set it up to scrape the metrics
    3. add a grafana container to docker compose, set it up to ingest the prometheus metrics
    4. build some basic visualizations of your prometheus metrics in grafana
    5. ask cursor to help you set up a situation that will exercise your prometheus code sufficiently to end up with enough metrics data that your graphs will have something interesting to show
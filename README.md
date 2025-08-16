# Bioprocessing

Process bioreactor sensor readings with anomaly detection.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Usage

```python
from bioprocessing import process_data

readings = [
    {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:00", "ph_value": 7.2, "temperature": 37.5},
    {"sensor_id": "BioR1", "timestamp": "2025-08-16 14:30", "ph_value": 7.1, "temperature": 45.0}
]

results = process_data(readings)
# Returns: {"BioR1": (7.15, 1, "2025-08-16 14:30")}
```

## Testing

```bash
pytest test_bioprocessing.py
```

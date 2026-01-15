# PySpark Playground

A sandbox environment for experimenting with Apache Spark using Python.

## Setup

**Important**: You must activate the virtual environment before running any PySpark code.

```bash
# Activate the virtual environment
source venv/bin/activate

# Verify installation
python -c "import pyspark; print(pyspark.__version__)"
```

## Quick Start

```python
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("Playground") \
    .master("local[*]") \
    .getOrCreate()

# Your code here

# Clean up
spark.stop()
```

## Requirements

- Python 3.12+
- PySpark 4.1.1

All dependencies are installed in the virtual environment. See `requirements.txt` for details.

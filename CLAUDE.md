# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

**CRITICAL**: This project requires a Python virtual environment to be activated before running any PySpark code.

Before working with this repository:

```bash
# Activate the virtual environment
source venv/bin/activate

# Verify PySpark is available
python -c "import pyspark; print(pyspark.__version__)"
```

If the virtual environment doesn't exist or PySpark isn't installed:

```bash
# Create virtual environment
python -m venv venv

# Activate it
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Dependencies

- Python 3.12+
- PySpark 4.1.1
- py4j 0.10.9.9 (PySpark dependency)

## Architecture

This is a PySpark playground/sandbox environment for experimenting with Apache Spark via Python. PySpark is used for distributed data processing and analytics.

### Key Concepts

- **SparkSession**: Entry point for Spark functionality. Create with `SparkSession.builder.appName("name").getOrCreate()`
- **RDDs**: Resilient Distributed Datasets - low-level Spark abstraction
- **DataFrames**: Higher-level abstraction for structured data processing (preferred for most use cases)
- **Datasets**: Typed version of DataFrames (Scala/Java only)

### Common PySpark Patterns

```python
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("AppName") \
    .master("local[*]") \
    .getOrCreate()

# Read data
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Process data
result = df.filter(df.column > 0).groupBy("category").count()

# Stop Spark (cleanup)
spark.stop()
```

## Development Notes

- Always activate the virtual environment before running Python scripts
- Use `master("local[*]")` for local development to use all available cores
- Remember to call `spark.stop()` to clean up resources
- PySpark jobs can be run directly with `python script.py` (no special spark-submit needed for local mode)

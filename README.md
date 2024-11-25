# Blockchain Data Aggregator

A sample data pipeline that processes blockchain transactions, converts cryptocurrency volume values to USD, and provides API endpoints for data visualization. Initial data assignment for Mehmet, using Python, GoogleCloud, and BigQuery/ClickHouse

## Overview

This pipeline:
1. Loads transaction data from CSV to Google Cloud Storage
2. Processes and loads data into BigQuery
3. Converts cryptocurrency amounts to USD using free CoinGecko API
4. Provides API endpoints for data visualization
5. Runs daily updates at 1am UTC

## Prerequisites

- Python 3.8+
- Google Cloud Platform account
- GCP Project with BigQuery and Cloud Storage enabled
- Service account with appropriate permissions:
  - BigQuery Admin
  - Storage Admin
  - Service Account User

## Installation

1. Fork or clone the repository:
```bash
git clone [repository-url]
cd blockchain-data-aggregator
```

2. Install required packages:

`pip install -r requirements.txt`

3. Set up configuration:

- Place your GCP service account credentials JSON file in config/credentials.json
- Update config/config.yaml with your project details and pathing if needed

## Usage

### 1. Initial Setup and Data Load

`python src/processor/main.py`

**Functions:**

- Create GCP buckets (if needed)
- Upload CSV data
- Create BigQuery dataset and table
- Process and load initial data

### 2. Convert Volume Values to USD

`python src/processor/price_conversion.py`

**Functions:**

- Fetch historical prices from CoinGecko
- Convert all amounts to USD
- Store prices in GCP bucket for future reference

### 3. Run API Endpoint

`python src/processor/api_endpoint.py`

**Endpoint Access:**

- Daily volumes: http://localhost:5000/daily-volumes
- Project volumes: http://localhost:5000/project-volumes

### 4. Enable Daily Updates

`python src/processor/scheduler.py`

Runs price updates daily at 1am UTC. Output is constant due to the fixed dates of the current sample data, in other circumstances price updates would be fetched.


## Data Validation

`python src/utils/validation.py`

The validation utility script ensures both that setup/loading was successful without unexpected null values, while also outputting the final USD volume totals of each project id. As part of testing an additional daily-volumes endpoint has been added to provide further summary stats as needed. The two endpoints queried include:

1. /daily-volumes

- Daily transaction volumes by currency
- Transaction counts
- Total USD volume

2. /project-volumes

- Project-level aggregation
- Total transactions per project
- Total USD volume per project

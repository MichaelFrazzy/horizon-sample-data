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

1. Clone the repository:
```bash
git clone [repository-url]
cd blockchain-data-aggregator
```

2. Install required packages:

`pip install -r requirements.txt`

3. Set up configuration:

- Place your GCP service account credentials JSON file in config/credentials.json
- Update config/config.yaml with your project details and pathing if needed

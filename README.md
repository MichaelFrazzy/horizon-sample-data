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


## Pipeline Process Details

### 1. Initial Data Processing
The pipeline begins with `main.py`, which:
- Validates and reads the CSV data
- Extracts required fields:
  - Timestamp (`ts`)
  - Project ID
  - Currency symbol from `props` JSON
  - Transaction value from `nums` JSON
- Aggregates transactions by date and project
- Creates the initial BigQuery table structure

### 2. Price Conversion Process
`price_conversion.py` handles cryptocurrency conversion:
- Identifies unique currency symbols (MATIC, SFL, USDC, USDC.E)
- Fetches historical prices from CoinGecko API
- Handles special cases:
  - MATIC values are converted from wei (division by 1e18)
  - USDC/USDC.E maintain 1:1 USD value
- Stores prices in GCP bucket for future reference
- Updates BigQuery with USD values

### 3. Data Storage
The BigQuery table structure:
```sql
CREATE TABLE marketplace_analytics.daily_metrics (
    date DATE NOT NULL,
    project_id INTEGER NOT NULL,
    num_transactions INTEGER NOT NULL,
    usd_volume FLOAT NOT NULL
)

```

### 4. Scheduling and Updates
The scheduler `scheduler.py`:

- Runs daily at 1 AM UTC
- Fetches new prices from CoinGecko
- Updates USD values in BigQuery
- Maintains price history in GCP bucket
- Logs all activities for monitoring


## Detailed Setup Instructions
### 1. GCP Project Setup
  1. Create a new project through the Google Cloud console
  2. Ensure required APIs are enabled:
     - BigQuery API
     - Cloud Storage API
  3. Create service account
     - IAM & Admin > Service Accounts
     - Create new service account
     - Add required roles:
       - BigQuery Admin
       - Storage Admin
       - Service Account User
  4. Download credentials JSON file
  5. Place in `config/credentials.json` or alter pathing to preferred location

### 2. Configuration Setup

1. Copy sample config:
      
```bash
cp config/config.yaml.example config/config.yaml
```

2. Update configuration values if using different warehouse
      
```yaml 
project:
  id: "your-project-id"
  bucket_name: "your-bucket-name"
  region: "US"

credentials:
  path: "config/credentials.json"

bigquery:
  dataset: "marketplace_analytics"
  table: "daily_metrics"

data:
  path: "data/sample_data.csv"
```

### 3. Envirorment Setup

1. Create Python virtual envirorment:

```bash
python -m venv venv
source venv\bin\activate  # venv/Scripts/activate for Windows
```
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Verify setup:

```bash
python src/utils/validation.py
```
## Monitoring and Maintenance

### Logging
- All components write to their respective logs
- Scheduler logs in `price_updates.log`
- API endpoint logs to console

### Data Validation
The `validation.py` script includes an initial query to conduct regular validation checks of the initial loading and config process. It's 2nd query output  totals in USD to confirm final data outputs once setup and normalization is complete.

Regular validation checks:

1. Run validation utility to verify:

   ```bash
   python src/utils/validation.py
   ```
2. Check for:
- Null USD values
- Transaction count accuracy
- Price update success
- Data consistency

## Common Issues and Their Solutions

### 1. CoinGecko Basic API Rate Limits
- Built-in delay between requests (1.2 seconds)
- Price caching in GCP bucket
- Fallback to last known/stored prices if needed

### 2. BigQuery Updates
- Automatic retry on failure
- Transaction logging for debugging
- Validation checks after updates

### 3. Missing Prices
- System falls back to last known price
- Logs warning for manual review
- Maintains price history in GCP bucket

## Development and Testing

### Running Tests

```bash
python src/utils/validation.py
```

As mentioned a section above, this script both validates the setup process and final output. The 2nd query during this final verification validates:
- Data integrity
- Volume price conversions
- Transaction and/or date count
- USD calculations

## Local Reproduction

**1. Set up local envirorment:**
   
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

**2. Use `sample_data.csv` under data directory for testing**
  
**3. Monitor logs for issues:**
   - Check `price_updates.log`
   - Review BigQuery logs
   - Monitor API endpoint responses

## Scheduler Deployment

**1. Set up scheduled job script:**

```bash
python src/processor/scheduler.py
```

**2. Configure logging:**
- Set appropriate log levels
- Define schedule
- Monitor usage


**3. Monitor:**
- API endpoint health
- Price update success
- Data consistency
- Resource usage

## Data Flow Order

**1. Initial Processing:**
- CSV → GCP Bucket
- Raw data → BigQuery
- JSON field extraction

**2. Price Conversion:**
- CoinGecko API calls
- Price storage in GCP
- USD value calculation

**3. Daily Updates:**
- Scheduled price fetching
- BigQuery updates
- Validation checks

**4. Data Access:**
- API endpoints
- BigQuery queries
- Validation tool

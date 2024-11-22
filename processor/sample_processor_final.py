# Cell 1, Imports

import os
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import json


credentials_path = r"C:\sample-processor\credentials\sample-data-processor-2d8488c675a5.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

# Verify the path
if os.path.exists(credentials_path):
    print(f"✓ Credentials file found at: {credentials_path}")
else:
    print(f"❌ No file found at: {credentials_path}")

# Cell 2, main scripts

class MarketplaceDataProcessor:
    def __init__(self, project_id, bucket_name):
        """Initialize processor with project and bucket names"""
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()
        
    def setup_gcs(self):
        """Setup Google Cloud Storage bucket"""
        try:
            bucket = self.storage_client.create_bucket(self.bucket_name)
            print(f"Bucket {self.bucket_name} created")
        except Exception as e:
            print(f"Using existing bucket: {self.bucket_name}")
            bucket = self.storage_client.bucket(self.bucket_name)
        return bucket
    
    def upload_to_gcs(self, source_file_path):
        """Upload local CSV to GCS"""
        bucket = self.storage_client.bucket(self.bucket_name)
        destination_blob = "raw/sample_data.csv"
        blob = bucket.blob(destination_blob)
        
        blob.upload_from_filename(source_file_path)
        print(f"File uploaded to gs://{self.bucket_name}/{destination_blob}")
        return f"gs://{self.bucket_name}/{destination_blob}"
    
    def setup_bigquery(self):
        """Setup BigQuery dataset and table"""
        dataset_id = f"{self.project_id}.marketplace_analytics"
        
        # Create dataset
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = self.bq_client.create_dataset(dataset, exists_ok=True)
        
        # Create table
        schema = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("project_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("num_transactions", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("usd_volume", "FLOAT", mode="REQUIRED"),
        ]
        
        table_id = f"{dataset_id}.daily_metrics"
        table = bigquery.Table(table_id, schema=schema)
        table = self.bq_client.create_table(table, exists_ok=True)
        
        print(f"Created table {table_id}")
        return table_id
        
    def process_data(self, gcs_path):
        """Process data from GCS and load to BigQuery"""
        # Read data from GCS
        df = pd.read_csv(gcs_path)
        
        # Transform data
        df['date'] = pd.to_datetime(df['ts']).dt.date
        df['currency_symbol'] = df['props'].apply(
            lambda x: json.loads(x).get('currencySymbol', '')
        )
        df['currency_value'] = df['nums'].apply(
            lambda x: float(json.loads(x).get('currencyValueDecimal', 0))
        )
        
        # Aggregate Data
        result = df.groupby(['date', 'project_id']).agg({
            'currency_value': ['count', 'sum']
        }).reset_index()
        
        # Clean up column names
        result.columns = ['date', 'project_id', 'num_transactions', 'usd_volume']
        
        return result

    def load_to_bigquery(self, df, table_id):
        """Load processed data to BigQuery"""
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND",
        )
        
        job = self.bq_client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()
        print(f"Loaded {len(df)} rows into {table_id}")


# Cell 3, Configuration
PROJECT_ID = 'sample-data-processor'  # Your project ID
BUCKET_NAME = f"{PROJECT_ID}-bucket"
LOCAL_CSV = 'C:\sample-processor\data\sample_data.csv'  # Relative path to your CSV

# Initialize processor
processor = MarketplaceDataProcessor(PROJECT_ID, BUCKET_NAME)

# Step 1: Setup infrastructure
print("Setting up infrastructure...")
processor.setup_gcs()
gcs_path = processor.upload_to_gcs(LOCAL_CSV)
table_id = processor.setup_bigquery()

# Step 2: Process and load data
print("\nProcessing data...")
processed_df = processor.process_data(LOCAL_CSV)

# Preview the processed data
print("\nProcessed Data Preview:")
display(processed_df.head())

# Step 3: Load to BigQuery
print("\nLoading to BigQuery...")
processor.load_to_bigquery(processed_df, table_id)

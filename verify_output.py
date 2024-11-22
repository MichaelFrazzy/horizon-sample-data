# Verify Data

from google.cloud import bigquery

def verify_data():
    client = bigquery.Client()
    query = """
    SELECT 
        date,
        project_id,
        num_transactions,
        usd_volume,
    FROM `sample-data-processor.marketplace_analytics.daily_metrics`
    ORDER BY date, project_id
    """
    
    results = client.query(query).to_dataframe()
    print("Data in BigQuery:")
    display(results)
    
    print("\nSummary Statistics:")
    print(f"Total Records: {len(results)}")
    print(f"Total Volume: {results['usd_volume'].sum():.2f}")
    print(f"Date Range: {results['date'].min()} to {results['date'].max()}")

verify_data()

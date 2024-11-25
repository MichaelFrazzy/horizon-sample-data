from google.cloud import bigquery
import os
import yaml

class DataValidator:
    def __init__(self):
        """Initialize validator using config"""
        # Load config
        with open('config/config.yaml', 'r') as f:
            config = yaml.safe_load(f)
            
        # Set credentials and project
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['credentials']['path']
        self.client = bigquery.Client()
        self.project_id = config['project']['id']
        
    def verify_setup(self):
        """Verify BigQuery table and data exist"""
        query = f"""
        SELECT COUNT(*) as count
        FROM `{self.project_id}.marketplace_analytics.daily_metrics`
        """
        
        try:
            result = self.client.query(query).result()
            row = next(result)
            print(f"✓ Found {row.count} rows in daily_metrics")
            return True
        except Exception as e:
            print(f"❌ Setup verification failed: {str(e)}")
            return False
    
    def verify_data(self):
        """Verify output"""
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT date) as unique_dates,
            COUNT(DISTINCT project_id) as unique_projects,
            SUM(CASE WHEN usd_volume IS NULL THEN 1 ELSE 0 END) as null_volumes
        FROM `{self.project_id}.marketplace_analytics.daily_metrics`
        """
        
        try:
            result = self.client.query(query).result()
            stats = next(result)
            print("\nData Validation Results:")
            print(f"Total Rows: {stats.total_rows}")
            print(f"Unique Dates: {stats.unique_dates}")
            print(f"Unique Projects: {stats.unique_projects}")
            print(f"Null Volumes: {stats.null_volumes}")
            return stats.null_volumes == 0
        except Exception as e:
            print(f"❌ Data validation failed: {str(e)}")
            return False

    # Output function
    def display_results(self):
        """Display final project totals and volumes"""
        query = f"""
        SELECT 
            project_id,
            currency_symbol,
            COUNT(*) as total_transactions,
            ROUND(SUM(usd_volume), 2) as total_project_volume
        FROM `{self.project_id}.marketplace_analytics.daily_metrics`
        GROUP BY project_id, currency_symbol
        ORDER BY project_id, currency_symbol
        """
        
        try:
            results = self.client.query(query).result()
            print("\nFinal Project Results:")
            print("----------------------")
            for row in results:
                print(f"Project {row.project_id} - {row.currency_symbol}:")
                print(f"  Transactions: {row.total_transactions}")
                print(f"  Total Volume: ${row.total_project_volume:,.2f}")
            return True
        except Exception as e:
            print(f"❌ Error displaying results: {str(e)}")
            return False
            
def run_validation():
    validator = DataValidator()
    setup_ok = validator.verify_setup()
    data_ok = validator.verify_data()
    print("\nDisplaying final results...")  # Output lines
    results_ok = validator.display_results()  
    return setup_ok and data_ok and results_ok  

if __name__ == "__main__":
    run_validation()

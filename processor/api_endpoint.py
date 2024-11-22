import os
from flask import Flask, jsonify
from google.cloud import bigquery
import yaml

app = Flask(__name__)

# Load config
with open('config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# Set up credentials
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config['credentials_path']

app = Flask(__name__)

@app.route('/daily-volumes', methods=['GET'])
def get_daily_volumes():
    client = bigquery.Client()
    query = """
    SELECT 
        DATE(date) as date,
        currency_symbol,
        COUNT(*) as transactions,
        ROUND(SUM(usd_volume), 2) as total_usd_volume
    FROM `sample-data-processor.marketplace_analytics.daily_metrics`
    GROUP BY date, currency_symbol
    ORDER BY date, currency_symbol
    """
    
    results = client.query(query).to_dataframe()
    return jsonify(results.to_dict(orient='records'))

@app.route('/project-volumes', methods=['GET'])
def get_project_volumes():
    client = bigquery.Client()
    query = """
    SELECT 
        project_id,
        currency_symbol,
        COUNT(*) as total_transactions,
        ROUND(SUM(usd_volume), 2) as total_project_volume
    FROM `sample-data-processor.marketplace_analytics.daily_metrics`
    GROUP BY project_id, currency_symbol
    ORDER BY project_id, currency_symbol
    """
    
    results = client.query(query).to_dataframe()
    return jsonify(results.to_dict(orient='records'))

# Open browser to endpoints after server starts
def open_browser():
    webbrowser.open('http://127.0.0.1:5000/daily-volumes')
    webbrowser.open('http://127.0.0.1:5000/project-volumes')

if __name__ == '__main__':
    Timer(1, open_browser).start()
    app.run(port=5000)

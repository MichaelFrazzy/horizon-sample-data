import requests
from datetime import datetime
import time
import pandas as pd
import json
from google.cloud import storage
from google.cloud import bigquery
import os

def setup_coingecko_conversion(config):
    client = bigquery.Client()
    storage_client = storage.Client()
    
    def store_price(symbol, date, price):
        """Store price in GCP bucket"""
        bucket = storage_client.bucket(config['bucket_name'])
        date_str = date.strftime('%Y-%m-%d')
        blob = bucket.blob(f'prices/{symbol}/{date_str}.json')
        blob.upload_from_string(json.dumps({'price': price}))
        
    def get_stored_price(symbol, date):
        try:
            bucket = storage_client.bucket(config['bucket_name'])
            date_str = date.strftime('%Y-%m-%d')
            blob = bucket.blob(f'prices/{symbol}/{date_str}.json')
            if blob.exists():
                data = json.loads(blob.download_as_string())
                return data.get('price')
        except:
            return None
        return None
        
    def get_coingecko_price(symbol, date):
        if symbol in ['USDC', 'USDC.E']:
            print(f"✓ Using 1.0 for {symbol}")
            return 1.0
            
        stored_price = get_stored_price(symbol, date)
        if stored_price:
            print(f"✓ Using stored price for {symbol} on {date}: ${stored_price}")
            return stored_price
            
        # Fetch CoinGecko
        coin_ids = {
            'MATIC': 'matic-network',
            'SFL': 'sunflower-land'
        }
        
        date_str = date.strftime('%d-%m-%Y')
        time.sleep(1.2)
        
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_ids[symbol]}/history"
            params = {'date': date_str, 'localization': 'false'}
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                price = data.get('market_data', {}).get('current_price', {}).get('usd', None)
                if price:
                    # Store price for future use
                    store_price(symbol, date, price)
                    print(f"✓ Got new price for {symbol} on {date_str}: ${price}")
                    return float(price)
            
            # use stored price if needed
            last_known = get_last_known_price(symbol, date)
            if last_known:
                return last_known
            
            print(f"! No price available for {symbol} on {date_str}")
            return None
        except Exception as e:
            print(f"! Error getting price for {symbol}: {str(e)}")
            return None
            
    def get_last_known_price(symbol, target_date):
        """Get last known price before target date"""
        bucket = storage_client.bucket(config['bucket_name'])
        blobs = bucket.list_blobs(prefix=f'prices/{symbol}/')
        
        last_price = None
        last_date = None
        
        for blob in blobs:
            date_str = blob.name.split('/')[-1].replace('.json', '')
            blob_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            
            if blob_date < target_date:
                data = json.loads(blob.download_as_string())
                price = data.get('price')
                
                if last_date is None or blob_date > last_date:
                    last_price = price
                    last_date = blob_date
        
        if last_price:
            print(f"✓ Using last known price from {last_date} for {symbol}")
        return last_price

    # Process data and update prices
    print("Processing source data...")
    df = pd.read_csv(config['data_path'])
    
    # Extract and process values
    df['date'] = pd.to_datetime(df['ts']).dt.date
    df['currency_symbol'] = df['props'].apply(lambda x: json.loads(x).get('currencySymbol', ''))
    df['decimal_value'] = df['nums'].apply(lambda x: float(json.loads(x).get('currencyValueDecimal', 0)))
    
    daily_volumes = df.groupby(['date', 'currency_symbol', 'project_id'])['decimal_value'].sum().reset_index()
    
    # Process each group
    for _, row in daily_volumes.iterrows():
        date = row['date']
        currency = row['currency_symbol']
        raw_volume = row['decimal_value']
        project_id = row['project_id']
        
        price = get_coingecko_price(currency, date)
        if price is None:
            continue
            
        # Calculate USD value
        if currency == 'MATIC':
            actual_volume = raw_volume / 1e18  # Convert from wei
            usd_value = actual_volume * price
        elif currency in ['USDC', 'USDC.E']:
            usd_value = raw_volume  # Stablecoins stay at face value
        else:
            usd_value = raw_volume * price
            
        update_query = f"""
        UPDATE `{config['project_id']}.marketplace_analytics.daily_metrics`
        SET usd_volume = {usd_value}
        WHERE DATE(date) = '{date}'
        AND currency_symbol = '{currency}'
        AND project_id = {project_id}
        """
        
        client.query(update_query)
        print(f"✓ Updated {currency} for {date}")
        print(f"  Raw Volume: {raw_volume:.2f}")
        if currency == 'MATIC':
            print(f"  Actual Volume: {actual_volume:.2f}")
        print(f"  Price: ${price}")
        print(f"  USD Value: ${usd_value:.2f}")
    
    # Final verify
    verify_query = f"""
    SELECT 
        DATE(date) as date,
        currency_symbol,
        COUNT(*) as transactions,
        SUM(usd_volume) as total_usd_volume
    FROM `{config['project_id']}.marketplace_analytics.daily_metrics`
    GROUP BY date, currency_symbol
    ORDER BY date, currency_symbol
    """
    
    print("\nFinal USD Volumes:")
    results = client.query(verify_query).to_dataframe()
    results['total_usd_volume'] = results['total_usd_volume'].apply(lambda x: f"{x:.2f}")
    print(results.to_string(index=False))
    return results

if __name__ == "__main__":
    config = {
        'project_id': 'sample-data-processor',
        'bucket_name': 'sample-data-processor-bucket',
        'data_path': 'data/sample_data.csv'
    }
    setup_coingecko_conversion(config)

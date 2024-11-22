from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import logging
import os
import sys
import yaml

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('price_updates.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processor.price_processor import setup_coingecko_conversion

def daily_price_update():
    """Daily price update job"""
    try:
        logger.info(f"Starting daily price update for {datetime.now().date()}")
        
        # Load config
        with open('config/config.yaml', 'r') as f:
            yaml_config = yaml.safe_load(f)
        
        # Convert yaml config to script format
        config = {
            'project_id': yaml_config['project']['id'],
            'bucket_name': yaml_config['project']['bucket_name'],
            'data_path': yaml_config['data']['path']
        }
        
        # Set credentials
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = yaml_config['credentials']['path']
        
        # Run price update
        setup_coingecko_conversion(config)
        logger.info("Daily price update completed successfully")
    except Exception as e:
        logger.error(f"Error in daily price update: {str(e)}")

def main():
    scheduler = BlockingScheduler()
    
    # Schedule job to run at 1 AM UTC
    scheduler.add_job(
        daily_price_update,
        'cron',
        hour=1,
        minute=0
    )
    
    try:
        logger.info("Starting scheduler")
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopping scheduler")
        scheduler.shutdown()

if __name__ == "__main__":
    main()

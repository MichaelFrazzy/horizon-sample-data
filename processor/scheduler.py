from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processor.main import setup_coingecko_conversion_dynamic

# Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('price_updates.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def daily_price_update():
    """Daily price update job"""
    try:
        logger.info(f"Starting daily price update for {datetime.now().date()}")
        setup_coingecko_conversion_dynamic()
        logger.info("Daily price update completed successfully")
    except Exception as e:
        logger.error(f"Error in daily price update: {str(e)}")

def main():
    scheduler = BlockingScheduler()
    
    # 1am UTC
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

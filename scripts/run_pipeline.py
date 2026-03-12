"""
Main ETL Pipeline
"""
import logging
import sys
import os
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.fetch_kobo_data import KoboFetcher
from etl.insert_postgres import DataInserter
from database.db_connection import db

# Create logs folder if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Setup logging
log_filename = f"logs/etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_pipeline(limit=None):
    """Run the ETL pipeline"""
    
    logger.info("="*50)
    logger.info("STARTING ETL PIPELINE")
    logger.info("="*50)
    
    stats = {
        'fetched': 0,
        'inserted': 0,
        'duplicates': 0
    }
    
    try:
        # Step 1: Check database
        logger.info("Step 1: Checking database...")
        if not db.test_connection():
            raise Exception("Database connection failed")
        logger.info("✅ Database OK")
        
        # Step 2: Fetch from Kobo
        logger.info("Step 2: Fetching from Kobo...")
        fetcher = KoboFetcher()
        submissions = fetcher.fetch_all(limit=limit)
        stats['fetched'] = len(submissions)
        logger.info(f"✅ Fetched {stats['fetched']} submissions")
        
        if not submissions:
            logger.info("No new data")
            return stats
        
        # Step 3: Insert into database
        logger.info("Step 3: Inserting into database...")
        inserter = DataInserter()
        inserted, duplicates = inserter.insert_submissions(submissions)
        stats['inserted'] = inserted
        stats['duplicates'] = duplicates
        
        # Summary
        logger.info("="*50)
        logger.info("PIPELINE COMPLETE")
        logger.info(f"Fetched: {stats['fetched']}")
        logger.info(f"Inserted: {stats['inserted']}")
        logger.info(f"Duplicates: {stats['duplicates']}")
        logger.info(f"Log file: {log_filename}")
        logger.info("="*50)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    
    return stats

if __name__ == "__main__":
    # Check for command line argument
    limit = None
    if len(sys.argv) > 1 and sys.argv[1] == '--limit':
        limit = int(sys.argv[2])
    
    run_pipeline(limit=limit)
"""
Create database tables
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database.db_connection import db
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_tables():
    """Create the price_monitoring table"""
    
    # SQL to create table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS price_monitoring (
        id SERIAL PRIMARY KEY,
        submission_uuid TEXT UNIQUE,
        submission_time TIMESTAMP,
        survey_date DATE,
        province TEXT,
        outlet_type TEXT,
        category_type TEXT,
        brand TEXT,
        sku TEXT,
        package_type TEXT,
        unit_per_ctn INTEGER,
        price_ws_buy_ctn NUMERIC(10,2),
        price_ws_sell_ctn NUMERIC(10,2),
        price_rt_sell_unit NUMERIC(10,2),
        gps_latitude NUMERIC(10,8),
        gps_longitude NUMERIC(11,8),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Create indexes for better performance
    create_indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_submission_time ON price_monitoring(submission_time);",
        "CREATE INDEX IF NOT EXISTS idx_survey_date ON price_monitoring(survey_date);",
        "CREATE INDEX IF NOT EXISTS idx_province ON price_monitoring(province);",
        "CREATE INDEX IF NOT EXISTS idx_brand ON price_monitoring(brand);"
    ]
    
    try:
        # Test connection first
        if not db.test_connection():
            logger.error("Cannot connect to database")
            return False
        
        # Create table
        with db.get_cursor() as cursor:
            logger.info("Creating table price_monitoring...")
            cursor.execute(create_table_sql)
            
            # Create indexes
            logger.info("Creating indexes...")
            for index_sql in create_indexes_sql:
                cursor.execute(index_sql)
            
            logger.info("✅ Table created successfully!")
            return True
            
    except Exception as e:
        logger.error(f"❌ Error creating table: {e}")
        return False

if __name__ == "__main__":
    print("="*50)
    print("Database Setup Script")
    print("="*50)
    create_tables()
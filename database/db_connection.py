"""
Database connection manager
"""
import psycopg2
import logging
from contextlib import contextmanager
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections"""
    
    def __init__(self):
        """Initialize with database configuration"""
        self.config = {
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        
        # Check for missing values
        missing = [k for k, v in self.config.items() if not v and k != 'port']
        if missing:
            logger.warning(f"Missing database configuration: {missing}")
    
    @contextmanager
    def get_connection(self):
        """Get database connection"""
        conn = None
        try:
            logger.debug("Connecting to database...")
            conn = psycopg2.connect(**self.config)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_cursor(self):
        """Get database cursor"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()
    
    def test_connection(self):
        """Test database connection"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

# Create single instance
db = DatabaseManager()

if __name__ == "__main__":
    # Test connection
    if db.test_connection():
        print("✅ Database connection successful!")
    else:
        print("❌ Database connection failed!")
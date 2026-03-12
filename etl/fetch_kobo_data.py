"""
Fetch data from Kobo API
"""
import requests
import logging
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.kobo_config import kobo_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KoboFetcher:
    """Fetch data from Kobo Toolbox API"""
    
    def __init__(self):
        """Initialize with API credentials"""
        self.config = kobo_config
        self.session = requests.Session()
        self.session.headers.update(self.config.headers)
    
    def fetch_all(self, limit=None):
        """
        Fetch all submissions
        """
        all_submissions = []
        url = self.config.data_url
        
        logger.info(f"Fetching from {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle response format
            if 'results' in data:
                submissions = data['results']
            else:
                submissions = data
            
            all_submissions.extend(submissions)
            
            # Apply limit if specified
            if limit and len(all_submissions) > limit:
                all_submissions = all_submissions[:limit]
            
            logger.info(f"✅ Fetched {len(all_submissions)} submissions")
            return all_submissions
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise
    
    def get_count(self):
        """Get total number of submissions"""
        try:
            response = self.session.get(
                self.config.data_url,
                params={'limit': 1},
                timeout=10
            )
            response.raise_for_status()
            data = response.json()
            
            if 'count' in data:
                return data['count']
            elif isinstance(data, list):
                return len(data)
            else:
                return 0
                
        except Exception as e:
            logger.error(f"Failed to get count: {e}")
            return -1

# For testing
if __name__ == "__main__":
    fetcher = KoboFetcher()
    count = fetcher.get_count()
    print(f"Submissions available: {count}")
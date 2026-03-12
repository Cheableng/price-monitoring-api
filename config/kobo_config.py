"""
Kobo API Configuration
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class KoboConfig:
    """Kobo API configuration"""
    
    def __init__(self):
        self.TOKEN = os.getenv('KOBO_TOKEN')
        self.ASSET_ID = os.getenv('ASSET_ID')
        self.BASE_URL = os.getenv('KOBO_BASE_URL', 'https://kf.kobotoolbox.org')
        
        # Print warning if credentials missing
        if not self.TOKEN:
            print("⚠️ WARNING: KOBO_TOKEN not found in .env file")
        if not self.ASSET_ID:
            print("⚠️ WARNING: ASSET_ID not found in .env file")
    
    @property
    def data_url(self):
        """Get the data API endpoint"""
        return f"{self.BASE_URL}/api/v2/assets/{self.ASSET_ID}/data/"
    
    @property
    def headers(self):
        """Get API headers with authentication"""
        return {
            "Authorization": f"Token {self.TOKEN}",
            "Content-Type": "application/json"
        }

# Create a single instance to use throughout the project
kobo_config = KoboConfig()

if __name__ == "__main__":
    print("Kobo Configuration Test:")
    print(f"Token: {kobo_config.TOKEN[:10]}...{kobo_config.TOKEN[-10:] if kobo_config.TOKEN else 'Not set'}")
    print(f"Asset ID: {kobo_config.ASSET_ID}")
    print(f"API URL: {kobo_config.data_url}")
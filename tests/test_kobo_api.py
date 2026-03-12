"""
Test Kobo API connection
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.kobo_config import kobo_config
import requests

def test_connection():
    """Test Kobo API connection"""
    
    print("="*50)
    print("Testing Kobo API Connection")
    print("="*50)
    print(f"API URL: {kobo_config.data_url}")
    print(f"Token: {kobo_config.TOKEN[:10]}...{kobo_config.TOKEN[-10:] if kobo_config.TOKEN else 'Not set'}")
    print(f"Asset ID: {kobo_config.ASSET_ID}")
    print("-"*50)
    
    try:
        response = requests.get(
            kobo_config.data_url,
            headers=kobo_config.headers,
            timeout=10
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            if 'results' in data:
                count = len(data['results'])
                print(f"✅ Success! Found {count} submissions")
                if count > 0:
                    print("\nSample fields:")
                    for key in list(data['results'][0].keys())[:10]:
                        print(f"  - {key}")
            else:
                print("✅ Connected but unexpected response")
        else:
            print(f"❌ Failed: {response.status_code}")
            print(response.text[:200])
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_connection()
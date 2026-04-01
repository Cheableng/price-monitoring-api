import requests
import json

KOBO_TOKEN = "b94292e24182df31967189a65377530f57ae5490"
ASSET_ID = "ajQ6Zhsfto9PteSbmsLnmK"
KOBO_URL = f"https://kf.kobotoolbox.org/api/v2/assets/{ASSET_ID}/data/?limit=1"

headers = {"Authorization": f"Token {KOBO_TOKEN}"}
response = requests.get(KOBO_URL, headers=headers)
data = response.json()

if 'results' in data and data['results']:
    submission = data['results'][0]
    print("="*60)
    print("COLUMNS IN YOUR KOBO DATA")
    print("="*60)
    
    # Show all fields that have values
    for key, value in submission.items():
        if value and not key.startswith('_'):
            print(f"{key}: {value}")
    
    # Specifically look for product type
    print("\n🔍 Looking for Product Type fields:")
    for key in submission.keys():
        if 'product' in key.lower() or 'type' in key.lower():
            print(f"  Possible product type field: {key} = {submission.get(key)}")
else:
    print("No submissions found")
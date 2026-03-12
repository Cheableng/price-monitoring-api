import requests
import json

KOBO_TOKEN = "712ea38442db1feb8191ac47fbad4292dc22eedf"
ASSET_ID = "az5eCmuhQRZzfgr4c6frK5"
KOBO_URL = f"https://kf.kobotoolbox.org/api/v2/assets/{ASSET_ID}/data/?limit=1"

headers = {"Authorization": f"Token {KOBO_TOKEN}"}
response = requests.get(KOBO_URL, headers=headers)
data = response.json()

print("="*60)
print("KOBO SUBMISSION DEBUG")
print("="*60)

if 'results' in data and data['results']:
    submission = data['results'][0]
    print(f"\n✅ Found 1 submission")
    print(f"UUID: {submission.get('_uuid', 'N/A')}")
    
    print("\n📋 ALL FIELDS IN THIS SUBMISSION:")
    print("-" * 40)
    
    # Group fields by category
    location_fields = []
    product_fields = []
    price_fields = []
    other_fields = []
    
    for key, value in submission.items():
        if value and str(value).strip():  # Only show fields with values
            key_lower = key.lower()
            if any(x in key_lower for x in ['province', 'outlet', 'gps', 'map', 'date']):
                location_fields.append((key, value))
            elif any(x in key_lower for x in ['price', 'buy', 'sell', 'rt', 'ctn', 'unit']):
                price_fields.append((key, value))
            elif any(x in key_lower for x in ['oil', 'sauce', 'milk', 'tissue', 'spray', 'coil', 'detergent', 'shampoo', 'cleaner', 'wash']):
                product_fields.append((key, value))
            elif not key.startswith('_'):
                other_fields.append((key, value))
    
    print("\n📍 LOCATION FIELDS:")
    for key, value in location_fields:
        print(f"  {key}: {value}")
    
    print("\n💰 PRICE FIELDS:")
    for key, value in price_fields:
        print(f"  {key}: {value}")
    
    print("\n📦 PRODUCT FIELDS:")
    for key, value in product_fields:
        print(f"  {key}: {value}")
    
    print("\n📝 OTHER FIELDS:")
    for key, value in other_fields[:10]:  # Show first 10
        print(f"  {key}: {value}")
    
    print("\n" + "="*60)
    
else:
    print("❌ No submissions found")
    print(f"Response: {json.dumps(data, indent=2)[:500]}")
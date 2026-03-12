import requests
import pandas as pd

# Kobo API config
KOBO_TOKEN = "712ea38442db1feb8191ac47fbad4292dc22eedf"
ASSET_ID = "az5eCmuhQRZzfgr4c6frK5"
KOBO_URL = f"https://kf.kobotoolbox.org/api/v2/assets/{ASSET_ID}/data/"

# Fetch data
headers = {"Authorization": f"Token {KOBO_TOKEN}"}
response = requests.get(KOBO_URL, headers=headers)
data = response.json()
results = data.get("results", [])

if results:
    # Create DataFrame
    df = pd.DataFrame(results)
    
    print("="*60)
    print("COLUMNS IN YOUR KOBO DATA")
    print("="*60)
    
    # Show all columns
    for i, col in enumerate(df.columns, 1):
        print(f"{i:2}. {col}")
    
    print("\n" + "="*60)
    print("FIRST ROW SAMPLE")
    print("="*60)
    
    # Show first row values for key columns
    first_row = results[0]
    key_columns = ['_uuid', '_submission_time', 'Province', 'Product Type', 'Brand']
    for col in key_columns:
        if col in first_row:
            print(f"{col}: {first_row[col]}")
        else:
            # Look for similar column names
            for k in first_row.keys():
                if 'date' in k.lower() or 'survey' in k.lower():
                    print(f"Possible date column: {k} = {first_row[k]}")
                if 'province' in k.lower():
                    print(f"Possible province column: {k} = {first_row[k]}")
else:
    print("No data found")
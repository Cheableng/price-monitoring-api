import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from datetime import datetime

# -------------------------------------------------
# KOBO API CONFIG
# -------------------------------------------------
KOBO_TOKEN = "712ea38442db1feb8191ac47fbad4292dc22eedf"
ASSET_ID = "az5eCmuhQRZzfgr4c6frK5"
KOBO_URL = f"https://kf.kobotoolbox.org/api/v2/assets/{ASSET_ID}/data/"

# -------------------------------------------------
# DATABASE CONFIG (SUPABASE)
# -------------------------------------------------
DB_HOST = "aws-0-ap-southeast-1.pooler.supabase.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "butcheableng"  # Your password
DB_PORT = "6543"

# -------------------------------------------------
# PRODUCT BRAND MASTER (COMPLETE FROM YOUR FORM)
# -------------------------------------------------
product_brands = {
    "Oil": [
        "Healthy Plus", "Simply", "Super Cooks", "Kencook", "Wim Wim Chef",
        "Heart", "Orchid", "Meizan Gold", "Cailan", "Happi Koki", "Cook"
    ],
    "Soy Sauce": [
        "Cow", "Leang Leng", "Chin Su", "HUON Viet", "Soya", "Sobi"
    ],
    "Oyster Sauce": [
        "Leang Leng", "Cheng Cheng", "Chomrong Chamroeun", "Hong Ty",
        "Lim Khi Hong", "Ra In", "Golden Crown", "Meng Leang",
        "Kim Eng", "Good Chef", "Tirk Breng Kjong"
    ],
    "Fish Can": [
        "Option 1", "Option 2", "Three Lady Cook", "Smiling Fish", "Ayam Brand"
    ],
    "Insect Spray": [
        "Ora", "Ranger Scout", "Jumbo Vape", "K-max Strong", "Stop",
        "Jolly", "Raid", "Revo", "Ars Jetblue", "Shieldtox", "Yunami"
    ],
    "Mosquito Coil": [
        "Ranger", "Jumbo Vape", "Jolly", "K-Max"
    ],
    "Condensed Milk": [
        "Phka Chhouk", "Best Cow", "Teapot", "Carnation", "Commander",
        "TIGA SAPI", "My Boy", "Indomilk", "Cow Milk"
    ],
    "Sterilized Milk": ["Option 1", "Option 2"],
    "Evaporate": ["Option 1", "Option 2"],
    "Dish Wash": [
        "Ora", "Sunlight", "Lix", "Power 100", "Beesoft", "Ora Light",
        "NAMFORT", "MYHAO", "Lii Mii", "ECONO", "IDOL", "Super Light", 
        "ALADANG", "Pinto", "Power 100 Saving", "Pudo"
    ],
    "Detergent Powder": [
        "Ora", "Viso", "Lix", "CS", "Powder 100", "Orie", "108",
        "Massen", "Cherin", "Fanlove", "168", "Wim Wim"
    ],
    "Detergent Liquid": [
        "Ora", "Fineline", "Viso", "Power 100", "Lix", "OCIIO",
        "Carefore", "Rins", "Pudu", "CHERIN", "LINDAN", "Pkmoria"
    ],
    "Baby Detergent": ["Ora", "Klen", "vitademo", "d-nee"],
    "Softener": [
        "Ora", "Comfort", "Hygiene", "Lix", "Downy"
    ],
    "Bleach": ["Ora", "OJavel"],
    "Tissue": [
        "Unity", "Champey", "Comfy", "Tessa", "Homelike", "L&M"
    ],
    "Body Shampoo": ["Option 1", "Option 2"],
    "Hand Wash": ["Option 1", "Option 2"],
    "Hair Shampoo": ["Sunsilk", "Klen", "Dove"],
    "Floor Cleaner": ["Beesoft", "Option 2", "Option 3"],
    "Toilet Cleaner": ["Ora", "Duck", "Sunwel", "Vim Vim", "Kundy Plus", "Lix"]
}

# -------------------------------------------------
# COLUMN MAPPING (UPDATED BASED ON YOUR EXCEL)
# -------------------------------------------------
column_mapping = {
    "_uuid": "submission_uuid",
    "_submission_time": "submission_time",
    "Enter Date": "survey_date",
    "Province": "province",
    "_Map_latitude": "gps_latitude",
    "_Map_longitude": "gps_longitude",
    "Outlet Types": "outlet_type",
    "Product Type": "product_type",
    "Brand": "brand",
    "SKU": "sku",
    "Package": "package_type",
    "Number unit in ctn": "unit_per_ctn",
    "Price Ws Buy in per ctn": "price_ws_buy_ctn",
    "Price Ws sell out per ctn": "price_ws_sell_ctn",
    "Price RT sell out per unit": "price_rt_sell_unit"
}

# -------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------
def clean_price(value):
    """Remove $ and convert to float"""
    if pd.isna(value) or value == "":
        return None
    try:
        if isinstance(value, str):
            value = value.replace('$', '').replace(',', '').strip()
        return float(value)
    except:
        return None

def clean_int(value):
    """Convert to int"""
    if pd.isna(value) or value == "":
        return None
    try:
        return int(float(value))
    except:
        return None

# -------------------------------------------------
# MAIN ETL PROCESS
# -------------------------------------------------
def run_etl():
    print("="*60)
    print("STARTING ETL PIPELINE")
    print("="*60)
    
    try:
        # 1. CONNECT TO DATABASE
        print("\n1. Connecting to database...")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = conn.cursor()
        print("✅ Database connected")
        
        # 2. FETCH DATA FROM KOBO
        print("\n2. Fetching from Kobo...")
        headers = {"Authorization": f"Token {KOBO_TOKEN}"}
        response = requests.get(KOBO_URL, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        results = data.get("results", [])
        print(f"✅ Fetched {len(results)} submissions")
        
        if not results:
            print("No data to process")
            return
        
        # 3. CONVERT TO DATAFRAME
        df = pd.DataFrame(results)
        
        # 4. SELECT AND RENAME COLUMNS
        available_columns = [col for col in column_mapping.keys() if col in df.columns]
        df = df[available_columns].copy()
        df = df.rename(columns=column_mapping)
        
        # 5. CLEAN DATA
        print("\n3. Cleaning data...")
        
        # Convert dates
        df['survey_date'] = pd.to_datetime(df['survey_date'], errors='coerce')
        df['submission_time'] = pd.to_datetime(df['submission_time'], errors='coerce')
        
        # Clean prices
        df['price_ws_buy_ctn'] = df['price_ws_buy_ctn'].apply(clean_price)
        df['price_ws_sell_ctn'] = df['price_ws_sell_ctn'].apply(clean_price)
        df['price_rt_sell_unit'] = df['price_rt_sell_unit'].apply(clean_price)
        df['unit_per_ctn'] = df['unit_per_ctn'].apply(clean_int)
        
        # 6. VALIDATE BRANDS
        print("\n4. Validating brands...")
        valid_rows = []
        invalid_brands = []
        
        for idx, row in df.iterrows():
            product_type = row.get('product_type')
            brand = row.get('brand')
            
            # Skip if missing critical data
            if pd.isna(product_type) or pd.isna(brand):
                continue
            
            # Check if brand is valid for this product type
            if product_type in product_brands:
                if brand in product_brands[product_type]:
                    valid_rows.append(row)
                else:
                    invalid_brands.append(f"{brand} ({product_type})")
            else:
                # Product type not in our list, but still include
                valid_rows.append(row)
        
        print(f"   Valid records: {len(valid_rows)}")
        if invalid_brands:
            print(f"   Invalid brands skipped: {len(invalid_brands)}")
            for brand in invalid_brands[:5]:  # Show first 5
                print(f"     - {brand}")
        
        if not valid_rows:
            print("No valid records to insert")
            return
        
        # Create dataframe of valid rows
        df_valid = pd.DataFrame(valid_rows)
        
        # 7. INSERT DATA
        print("\n5. Inserting into database...")
        
        insert_query = """
        INSERT INTO price_monitoring (
            submission_uuid, submission_time, survey_date,
            province, gps_latitude, gps_longitude, outlet_type,
            product_type, brand, sku, package_type,
            unit_per_ctn, price_ws_buy_ctn, price_ws_sell_ctn, price_rt_sell_unit
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (submission_uuid) DO UPDATE SET
            price_rt_sell_unit = EXCLUDED.price_rt_sell_unit,
            price_ws_buy_ctn = EXCLUDED.price_ws_buy_ctn,
            price_ws_sell_ctn = EXCLUDED.price_ws_sell_ctn,
            updated_at = CURRENT_TIMESTAMP
        """
        
        inserted = 0
        for _, row in df_valid.iterrows():
            try:
                cursor.execute(insert_query, (
                    row['submission_uuid'],
                    row['submission_time'],
                    row['survey_date'],
                    row['province'],
                    row['gps_latitude'],
                    row['gps_longitude'],
                    row['outlet_type'],
                    row['product_type'],
                    row['brand'],
                    row['sku'],
                    row['package_type'],
                    row['unit_per_ctn'],
                    row['price_ws_buy_ctn'],
                    row['price_ws_sell_ctn'],
                    row['price_rt_sell_unit']
                ))
                inserted += 1
            except Exception as e:
                print(f"Error inserting row: {e}")
        
        conn.commit()
        print(f"✅ Inserted: {inserted} records")
        
        # 8. SUMMARY
        print("\n" + "="*60)
        print("ETL COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"Total submissions: {len(results)}")
        print(f"Valid records inserted: {inserted}")
        print(f"Invalid brands skipped: {len(invalid_brands)}")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            print("\nDatabase connection closed")

# -------------------------------------------------
# RUN THE ETL
# -------------------------------------------------
if __name__ == "__main__":
    run_etl()
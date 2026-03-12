"""
Pandas ETL script for Kobo to Supabase - COMPLETE WITH ALL BRAND MAPPINGS
"""
import pandas as pd
import requests
import psycopg2
import numpy as np
from datetime import datetime

# -------------------------------------------------
# KOBO API CONFIG
# -------------------------------------------------
KOBO_TOKEN = "712ea38442db1feb8191ac47fbad4292dc22eedf"
ASSET_ID = "az5eCmuhQRZzfgr4c6frK5"
KOBO_URL = f"https://kf.kobotoolbox.org/api/v2/assets/{ASSET_ID}/data/"

# -------------------------------------------------
# DATABASE CONFIG
# -------------------------------------------------
DB_HOST = "aws-1-ap-southeast-1.pooler.supabase.com"
DB_NAME = "postgres"
DB_USER = "postgres.wfhbxjthmkkqvcuinmnw"
DB_PASSWORD = "butcheableng"
DB_PORT = "5432"

# -------------------------------------------------
# CURRENCY CONFIGURATION
# -------------------------------------------------
KHR_TO_USD = 4000

def clean_price(value, currency_type='usd'):
    """Clean price and convert if needed"""
    if pd.isna(value) or value == "" or value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace('$', '').replace(',', '').strip()
            if value == "":
                return None
        amount = float(value)
        if currency_type == 'khr':
            usd_amount = round(amount / KHR_TO_USD, 2)
            print(f"      💱 {amount:,.0f} KHR → ${usd_amount:.2f} USD")
            return usd_amount
        else:
            return amount
    except:
        return None

def clean_int(value):
    """Convert to int"""
    if pd.isna(value) or value == "" or value is None:
        return None
    try:
        return int(float(value))
    except:
        return None

def parse_gps(gps_string):
    """Parse GPS string into lat/lon"""
    if pd.isna(gps_string) or not gps_string:
        return None, None
    try:
        parts = str(gps_string).strip().split()
        if len(parts) >= 2:
            return float(parts[0]), float(parts[1])
    except:
        pass
    return None, None

# -------------------------------------------------
# PRODUCT TYPE MAPPING
# -------------------------------------------------
product_type_mapping = {
    'oil': 'Oil',
    'soy_sauce': 'Soy Sauce',
    'oyster_sauce': 'Oyster Sauce',
    'fish_can': 'Fish Can',
    'insect_spray': 'Insect Spray',
    'mosquito_coil': 'Insect Mosquito Coil',
    'condensed_milk': 'Condensed Milk',
    'sterilized_milk': 'Sterilized Milk',
    'evaporate': 'Evaporate',
    'dish_wash': 'Dish Wash',
    'detergent_powder': 'Detergent Powder',
    'detergent_liquid': 'Detergent Liquid',
    'baby_detergent': 'Baby Detergent',
    'softener': 'Softener',
    'bleach': 'Bleach',
    'tissue': 'Tissue',
    'hand_wash': 'Hand Wash',
    'hair_shampoo': 'Hair Shampoo',
    'floor_cleaner': 'Floor Cleaner',
    'toilet_cleaner': 'Toilet Cleaner',
    'body_shampoo': 'Body Shampoo'
}

# -------------------------------------------------
# COLUMN MAPPING FOR EACH PRODUCT TYPE
# -------------------------------------------------
def get_brand_column(product_type):
    """Return the correct column name for each product type"""
    column_map = {
        'oil': 'Oil',
        'soy_sauce': 'Soy_Sauce',
        'oyster_sauce': 'Oyster_Sauce',
        'fish_can': 'Fish_Can',
        'insect_spray': 'Insect_Spray',
        'mosquito_coil': 'Insect_Mosquito_Coil',
        'condensed_milk': 'Condensed_Milk',
        'sterilized_milk': 'Sterilized_Milk',
        'evaporate': 'Evaporate',
        'dish_wash': 'Dish_Wash',
        'detergent_powder': 'Detergent_Powder',
        'detergent_liquid': 'Detergent_Liquid',
        'baby_detergent': 'Baby_Detergent',
        'softener': 'Softener',
        'bleach': 'Bleach',
        'tissue': 'Tissue',
        'hand_wash': 'Hand_Wash',
        'hair_shampoo': 'Hair_Shampoo',
        'floor_cleaner': 'Floor_Cleaner',
        'toilet_cleaner': 'Toilet_Cleaner',
        'body_shampoo': 'Body_Shampoo'
    }
    return column_map.get(product_type, 'Brand')

# -------------------------------------------------
# PRODUCT-SPECIFIC BRAND MAPPING
# Based on ALL your Kobo form screenshots
# -------------------------------------------------
def get_product_brand(product_type, raw_brand):
    """Return the correct brand name based on product type"""
    
    if pd.isna(raw_brand) or raw_brand is None:
        return None
    
    raw_brand_str = str(raw_brand).strip()
    raw_brand_lower = raw_brand_str.lower()
    
    # ===== OIL BRANDS =====
    if product_type == 'Oil':
        oil_brands = {
            'healthy_plus': 'Healthy Plus',
            'simply': 'Simply',
            'super_cooks': 'Super Cooks',
            'kencook': 'Kencook',
            'wim_wim_chef': 'Wim Wim Chef',
            'heart': 'Heart',
            'orchid': 'Orchid',
            'meizan_gold': 'Meizan Gold',
            'cailan': 'Cailan',
            'happi_koki': 'Happi Koki',
            'cook': 'Cook',
        }
        return oil_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== SOY SAUCE BRANDS =====
    elif product_type == 'Soy Sauce':
        soy_brands = {
            'cow': 'Cow',
            'leang_leng': 'Leang Leng',
            'chin_su': 'Chin Su',
            'huon_viet': 'HUON Viet',
            'soya': 'Soya',
            'sobi': 'Sobi',
            'chamka_dong': 'Chamka Dong',
            'chin_white_cap': 'Chin White Cap',
            'chin_su_red_cap': 'Chin Su Red Cap',
            'mashi': 'Mashi',
            'haday': 'Haday',
        }
        return soy_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== OYSTER SAUCE BRANDS =====
    elif product_type == 'Oyster Sauce':
        oyster_brands = {
            'leang_leng': 'Leang Leng',
            'cheng_cheng': 'Cheng Cheng',
            'chomrong_chamroeun': 'Chomrong Chamroeun',
            'hong_ty': 'Hong Ty',
            'lim_khi_hong': 'Lim Khi Hong',
            'ra_in': 'Ra In',
            'golden_crown': 'Golden Crown',
            'meng_leang': 'Meng Leang',
            'kim_eng': 'Kim Eng',
            'good_chef': 'Good Chef',
            'tirk_breng_kjong': 'Tirk Breng Kjong',
            'e_che_ngov_heng': 'E Che Ngov Heng',
            'healthy_boy': 'Healthy Boy',
            'golden_mountain': 'Golden Mountain',
            'chua_hah_seng': 'Chua Hah Seng',
            'panda': 'Panda',
            'megachef': 'Megachef',
            'heng_heng': 'Heng Heng',
            'ay_hoa': 'Ay Hoa',
            'chay_sinh': 'Chay Sinh',
            'maekrua': 'Maekrua',
        }
        return oyster_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== FISH CAN BRANDS =====
    elif product_type == 'Fish Can':
        fish_brands = {
            'yoe\'s': 'Yoe\'s',
            'yoes': 'Yoe\'s',
            'win_chef': 'Win Chef',
            'lilly': 'Lilly',
            'you': 'You',
            'kitchen_chef': 'Kitchen Chef',
            'asahi': 'Asahi',
        }
        return fish_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== INSECT SPRAY BRANDS =====
    elif product_type == 'Insect Spray':
        spray_brands = {
            'ora': 'Ora',
            'ranger_scout': 'Ranger Scout',
            'jumbo_vape': 'Jumbo Vape',
            'k_max_strong': 'K-max Strong',
            'stop': 'Stop',
            'jolly': 'Jolly',
            'raid': 'Raid',
            'revo': 'Revo',
            'ars_jetblue': 'Ars Jetblue',
            'shieldtox': 'Shieldtox',
            'yunami': 'Yunami',
        }
        return spray_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== INSECT MOSQUITO COIL BRANDS =====
    elif product_type == 'Insect Mosquito Coil':
        coil_brands = {
            'ranger': 'Ranger',
            'jumbo_vape': 'Jumbo Vape',
            'jolly': 'Jolly',
            'k_max': 'K-Max',
        }
        return coil_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== CONDENSED MILK BRANDS =====
    elif product_type == 'Condensed Milk':
        milk_brands = {
            'phka_chhouk': 'Phka Chhouk',
            'best_cow': 'Best Cow',
            'teapot': 'Teapot',
            'carnation': 'Carnation',
            'commander': 'Commander',
            'tiga_sapi': 'TIGA SAPI',
            'my_boy': 'My Boy',
            'indomilk': 'Indomilk',
            'cow_milk': 'Cow Milk',
        }
        return milk_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== STERILIZED MILK BRANDS =====
    elif product_type == 'Sterilized Milk':
        sterilized_brands = {
            'bear_brand': 'Bear Brand',
            'angkormilk': 'Angkormilk',
            'vitaminik': 'Vitaminik',
            'mother_dairy': 'Mother Dairy',
            'east_field': 'East Field',
        }
        return sterilized_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== EVAPORATE BRANDS =====
    elif product_type == 'Evaporate':
        evaporate_brands = {
            'almas': 'Almas',
            'carnation': 'Carnation',
            'teapot': 'Teapot',
        }
        return evaporate_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== DISH WASH BRANDS =====
    elif product_type == 'Dish Wash':
        dish_brands = {
            'ora': 'Ora',
            'sunlight': 'Sunlight',
            'sunlight_1': 'Sunlight',
            'lix': 'Lix',
            'power_100': 'Power 100',
            'beesoft': 'Beesoft',
            'ora_light': 'Ora Light',
            'namfort': 'NAMFORT',
            'myhao': 'MYHAO',
            'li_mi': 'Li Mi',
            'eco': 'ECO',
            'idol': 'IDOL',
            'super_light': 'Super Light',
            'aladang': 'ALADANG',
            'pinto': 'Pinto',
            'power_100_saving': 'Power 100 Saving',
            'pudo': 'Pudo',
        }
        return dish_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== DETERGENT POWDER BRANDS =====
    elif product_type == 'Detergent Powder':
        powder_brands = {
            'ora': 'Ora',
            'viso': 'Viso',
            'lix': 'Lix',
            'cs': 'CS',
            'powder_100': 'Powder 100',
            'orie': 'Orie',
            '108': '108',
            'massen': 'Massen',
            'wassen': 'Wassen',
            'cherin': 'Cherin',
            'fanlove': 'Fanlove',
            '168': '168',
            'wim_wim': 'Wim Wim',
            'wifi_whi': 'Wifi Whi',
            'rlueline': 'Rlueline',
        }
        return powder_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== DETERGENT LIQUID BRANDS =====
    elif product_type == 'Detergent Liquid':
        liquid_brands = {
            'ora': 'Ora',
            'fineline': 'Fineline',
            'viso': 'Viso',
            'power_100': 'Power 100',
            'lix': 'Lix',
            'ocilio': 'Ocilio',
            'carefore': 'Carefore',
            'rins': 'Rins',
            'pudu': 'Pudu',
            'cherin': 'CHERIN',
            'lindan': 'LINDAN',
            'pkmoria': 'Pkmoria',
            'lii_mi': 'Lii-Mi',
            'surf': 'Surf',
            'kundy': 'Kundy',
            'chenry': 'Chenry',
            'k_ryta': 'K.Ryta',
            'rlueline': 'Rlueline',
        }
        return liquid_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== BABY DETERGENT BRANDS =====
    elif product_type == 'Baby Detergent':
        baby_brands = {
            'ora': 'Ora',
            'klen': 'Klen',
            'vitademo': 'Vitademo',
            'd_nee': 'D-nee',
        }
        return baby_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== SOFTENER BRANDS =====
    elif product_type == 'Softener':
        softener_brands = {
            'ora': 'Ora',
            'comfort': 'Comfort',
            'hygiene': 'Hygiene',
            'siusop': 'Siusop',
            'lix': 'Lix',
            'downy': 'Downy',
            'bukheang': 'Bukheang',
            'natha': 'Natha',
        }
        return softener_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== BLEACH BRANDS =====
    elif product_type == 'Bleach':
        bleach_brands = {
            'ora': 'Ora',
            'o\'javel': 'O\'Javel',
            'ojavel': 'O\'Javel',
        }
        return bleach_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== TOILET CLEANER BRANDS =====
    elif product_type == 'Toilet Cleaner':
        toilet_brands = {
            'ora': 'Ora',
            'duck': 'Duck',
            'sunwel': 'Sunwel',
            'vim_vim': 'Vim Vim',
            'kundy_plus': 'Kundy Plus',
            'lix': 'Lix',
        }
        return toilet_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== TISSUE BRANDS =====
    elif product_type == 'Tissue':
        tissue_brands = {
            'unity': 'Unity',
            'champey': 'Champey',
            'comfy': 'Comfy',
            'tessa': 'Tessa',
            'homelike': 'Homelike',
            'l_m': 'L&M',
        }
        return tissue_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== BODY SHAMPOO BRANDS =====
    elif product_type == 'Body Shampoo':
        body_brands = {
            'lux': 'LUX',
            'dov': 'Dov',
            'shokubutsu': 'Shokubutsu',
            'klen': 'Klen',
            'empress': 'Empress',
            'nivea': 'Nivea',
            'feira': 'Feira',
            'tamarind': 'Tamarind',
            'lom_ang': 'Lom Ang',
            'bouncia': 'Bouncia',
            'l\'hong': 'L\'hong',
            'ttm': 'TTM',
            'emory': 'Emory',
            'amony': 'Amony',
            'jasmine_rice': 'Jasmine Rice',
            'ng': 'NG',
            'okavi': 'Okavi',
            'tana': 'Tana',
            'aloda': 'Aloda',
            'neang_lor': 'Neang Lor',
            'preah_chan': 'Preah Chan',
            'protex': 'Protex',
            'pamolive': 'Pamolive',
            'romano': 'Romano',
            'natures_spa': 'Natures Spa',
            'dettol': 'Dettol',
            'sofine': 'Sofine',
            'ocilo': 'Ocilo',
            'ocio': 'Ocio',
            'elizzer': 'Elizzer',
            'sanex': 'Sanex',
            'adidas': 'Adidas',
            'bosba': 'Bosba',
            'citra': 'Citra',
            'bodia': 'Bodia',
            'enchanteur': 'Enchanteur',
            'apsor': 'Apsor',
            'yura_in_white': 'Yura in White',
            'eak_reach': 'Eak Reach',
            'ketotosc': 'Ketotosc',
            'apsara': 'Apsara',
        }
        return body_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== HAND WASH BRANDS =====
    elif product_type == 'Hand Wash':
        hand_brands = {
            'dettol': 'Dettol',
            'lifebuoy': 'Lifebuoy',
            'safeguard': 'Safeguard',
            'dove': 'Dove',
            'palmolive': 'Palmolive',
            'lux': 'Lux',
            'watsons': 'Watsons',
            'softsoap': 'Softsoap',
            'protex': 'Protex',
            'kambio_nature': 'Kambio Nature',
            'carex': 'Carex',
            'himalaya': 'Himalaya',
            'enchanteur': 'Enchanteur',
            'shokubutsu': 'Shokubutsu',
            'beauty_garde': 'Beauty Garde',
            'guardian': 'Guardian',
            'kirei_kirei': 'Kirei Kirei',
            'ociio': 'Ociio',
        }
        return hand_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== HAIR SHAMPOO BRANDS =====
    elif product_type == 'Hair Shampoo':
        hair_brands = {
            'sunsilk': 'Sunsilk',
            'klen': 'Klen',
            'dove': 'Dove',
            'pantene': 'Pantene',
            'head & shoulders': 'Head & Shoulders',
            'head_&_shoulders': 'Head & Shoulders',
            'clear': 'Clear',
            'tresemme': 'Tresemme',
            'rejoice': 'Rejoice',
            'herbal_essences': 'Herbal Essences',
            'senteurs_d\'angkor': 'Senteurs d\'Angkor',
            'senteurs_dangkor': 'Senteurs d\'Angkor',
            'kambio_nature': 'Kambio Nature',
            'u_well': 'U-Well',
            'romano': 'Romano',
            'gatsby': 'Gatsby',
            'biolane': 'Biolane',
            'adidas': 'Adidas',
            'st_khmer': 'ST Khmer',
            'moustidose': 'Moustidose',
        }
        return hair_brands.get(raw_brand_lower, raw_brand_str)
    
    # ===== FLOOR CLEANER BRANDS =====
    elif product_type == 'Floor Cleaner':
        floor_brands = {
            'beesoft': 'Beesoft',
            'domex': 'Domex',
            'clorox': 'Clorox',
            'sunlight': 'Sunlight',
            'lifebuoy': 'Lifebuoy',
            'saat': 'Saat',
            'lor': 'LOR',
            'vim': 'Vim',
            'ajax': 'Ajax',
            'seasons': 'Seasons',
            'mr_muscle': 'Mr.Muscle',
            'aro': 'Aro',
            'handy': 'Handy',
            'sunwel': 'Sunwel',
            'ring': 'Ring',
            'natha': 'Natha',
            'zip': 'Zip',
            'jackie': 'Jackie',
            'whiz': 'Whiz',
        }
        return floor_brands.get(raw_brand_lower, raw_brand_str)
    
    # For any other product type, return the original brand
    else:
        return raw_brand_str

# -------------------------------------------------
# MAIN ETL FUNCTION
# -------------------------------------------------
def run_etl():
    print("="*60)
    print("STARTING PANDAS ETL PIPELINE")
    print("="*60)
    
    try:
        # 1. FETCH FROM KOBO
        print("\n1. Fetching from Kobo API...")
        headers = {"Authorization": f"Token {KOBO_TOKEN}"}
        response = requests.get(KOBO_URL, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        results = data.get("results", [])
        print(f"✅ Fetched {len(results)} submissions")
        
        if not results:
            print("No data to process")
            return
        
        # 2. CONVERT TO DATAFRAME
        df = pd.DataFrame(results)
        print(f"✅ DataFrame created with {len(df)} rows")
        
        # 3. PROCESS EACH ROW
        print("\n2. Processing data...")
        
        processed_rows = []
        
        for idx, row in df.iterrows():
            try:
                # Parse GPS
                gps_string = row.get('Map')
                lat, lon = parse_gps(gps_string)
                
                # Get raw product type from Kobo
                raw_product_type = row.get('Product_Type')
                
                # Skip if no product type
                if not raw_product_type or pd.isna(raw_product_type):
                    print(f"  ⚠️ Row {idx}: No product type")
                    continue
                
                # Convert product type to display name
                if raw_product_type in product_type_mapping:
                    display_product_type = product_type_mapping[raw_product_type]
                    print(f"\n  🔄 Product: {raw_product_type} → {display_product_type}")
                else:
                    display_product_type = raw_product_type
                
                # Get the correct column for this product's brand
                brand_column = get_brand_column(raw_product_type)
                raw_brand = row.get(brand_column)
                
                # If no brand found, try alternative column names
                if not raw_brand or pd.isna(raw_brand):
                    # Try with spaces instead of underscores
                    alt_column = raw_product_type.replace('_', ' ').title()
                    raw_brand = row.get(alt_column)
                
                if not raw_brand or pd.isna(raw_brand):
                    # Try direct brand field
                    raw_brand = row.get('Brand')
                
                # Skip if no brand found
                if not raw_brand or pd.isna(raw_brand) or str(raw_brand).strip() == '':
                    print(f"  ⚠️ Row {idx}: No brand found for {display_product_type}")
                    continue
                
                # Get the correct brand using product-specific mapping
                display_brand = get_product_brand(display_product_type, raw_brand)
                
                if str(raw_brand).strip() != display_brand:
                    print(f"  🔄 Brand: {raw_brand} → {display_brand}")
                
                print(f"  📦 Final: {display_product_type} - {display_brand}")
                
                # Get price data
                price_buy = clean_price(row.get('Price_Ws_Buy_in_per_ctn'), 'usd')
                price_sell = clean_price(row.get('Price_Ws_sell_out_per_ctn'), 'usd')
                
                # Retail price (KHR → USD)
                retail_raw = row.get('Price_RT_sell_out_per_unit')
                if retail_raw:
                    print(f"     Retail raw: {retail_raw} KHR")
                price_retail_usd = clean_price(retail_raw, 'khr')
                
                # Other fields
                unit = clean_int(row.get('Number_unit_in_ctn'))
                
                # Create row data
                processed_rows.append({
                    'submission_uuid': row.get('_uuid'),
                    'submission_time': row.get('_submission_time'),
                    'survey_date': row.get('Enter_Date'),
                    'province': row.get('Province'),
                    'gps_latitude': lat,
                    'gps_longitude': lon,
                    'outlet_type': row.get('Outlet_Types'),
                    'product_type': display_product_type,
                    'brand': display_brand,
                    'sku': row.get('SKU'),
                    'package_type': row.get('Package'),
                    'unit_per_ctn': unit,
                    'price_ws_buy_ctn': price_buy,
                    'price_ws_sell_ctn': price_sell,
                    'price_rt_sell_unit': price_retail_usd
                })
                
            except Exception as e:
                print(f"  ❌ Error processing row {idx}: {e}")
        
        print(f"\n✅ Processed {len(processed_rows)} products")
        
        if not processed_rows:
            print("No valid data to insert")
            return
        
        # 4. CONNECT TO DATABASE
        print("\n3. Connecting to database...")
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        cursor = conn.cursor()
        print("✅ Database connected")
        
        # 5. INSERT DATA
        print("\n4. Inserting into database...")
        
        insert_query = """
        INSERT INTO price_monitoring (
            submission_uuid, submission_time, survey_date,
            province, gps_latitude, gps_longitude, outlet_type,
            product_type, brand, sku, package_type,
            unit_per_ctn, price_ws_buy_ctn, price_ws_sell_ctn, price_rt_sell_unit
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (submission_uuid, product_type, brand) 
        DO UPDATE SET
            price_rt_sell_unit = EXCLUDED.price_rt_sell_unit,
            updated_at = CURRENT_TIMESTAMP
        """
        
        inserted = 0
        updated = 0
        for row in processed_rows:
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
                if cursor.rowcount == 1:
                    inserted += 1
                    print(f"  ✅ Inserted: {row['product_type']} - {row['brand']}")
                elif cursor.rowcount == 2:
                    updated += 1
                    print(f"  🔄 Updated: {row['product_type']} - {row['brand']}")
                
            except Exception as e:
                print(f"  ❌ Error: {e}")
                continue
        
        conn.commit()
        print(f"\n✅ Results: {inserted} inserted, {updated} updated")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            print("\nDatabase connection closed")

if __name__ == "__main__":
    run_etl()
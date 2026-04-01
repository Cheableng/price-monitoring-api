"""
Kobo Webhook Receiver API - Lightweight Version
"""
from flask import Flask, request, jsonify
import psycopg2
import os
from datetime import datetime

app = Flask(__name__)

# Database config
DB_HOST = "aws-1-ap-southeast-1.pooler.supabase.com"
DB_NAME = "postgres"
DB_USER = "postgres.eokvgfohmbcoyisuptdp"
DB_PASSWORD = "butcheableng"
DB_PORT = "5432"
KHR_TO_USD = 4000

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def parse_gps(gps_str):
    """Parse GPS string like '11.565229 104.915439 0 0'"""
    if not gps_str:
        return None, None
    try:
        parts = gps_str.strip().split()
        if len(parts) >= 2:
            return float(parts[0]), float(parts[1])
    except:
        pass
    return None, None

@app.route('/webhook', methods=['POST'])
def kobo_webhook():
    try:
        data = request.json
        
        # Parse GPS
        lat, lon = parse_gps(data.get('gps'))
        
        # Convert retail price
        retail_khr = data.get('price_rt_sell')
        retail_usd = None
        if retail_khr:
            retail_usd = round(float(retail_khr) / KHR_TO_USD, 2)
            print(f"💱 {retail_khr} KHR → ${retail_usd} USD")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO price_monitoring (
                submission_time, survey_date, province, 
                gps_latitude, gps_longitude, outlet_type,
                product_type, brand, sku, package_type,
                unit_per_ctn, price_ws_buy_ctn, price_ws_sell_ctn, price_rt_sell_unit
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            datetime.now(),
            data.get('enter_date'),
            data.get('province'),
            lat, lon,
            data.get('outlet_types'),
            data.get('product_type'),
            data.get('brand'),
            data.get('sku'),
            data.get('package'),
            data.get('unit_per_ctn'),
            data.get('price_ws_buy'),
            data.get('price_ws_sell'),
            retail_usd
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Inserted: {data.get('product_type')} - {data.get('brand')}")
        return jsonify({"status": "success"}), 200
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
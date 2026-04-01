"""
Kobo Webhook Receiver API
Receives data from Kobo and inserts into Supabase
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

# Currency conversion
KHR_TO_USD = 4000

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

@app.route('/webhook', methods=['POST'])
def kobo_webhook():
    """
    Endpoint that Kobo calls when a new submission is made
    """
    try:
        # Get data from Kobo webhook
        data = request.json
        print(f"📥 Received webhook at {datetime.now()}")
        print(f"📦 Data: {data}")
        
        # Extract fields (adjust these based on your Kobo form)
        submission = {
            'submission_time': datetime.now(),
            'survey_date': data.get('enter_date'),
            'province': data.get('province'),
            'outlet_type': data.get('outlet_types'),
            'product_type': data.get('product_type'),
            'brand': data.get('brand'),
            'sku': data.get('sku'),
            'package_type': data.get('package'),
            'unit_per_ctn': data.get('unit_per_ctn'),
            'price_ws_buy_ctn': data.get('price_ws_buy'),
            'price_ws_sell_ctn': data.get('price_ws_sell'),
            'price_rt_sell_unit': data.get('price_rt_sell')
        }
        
        # Convert KHR to USD if needed
        if submission['price_rt_sell_unit']:
            try:
                price_khr = float(submission['price_rt_sell_unit'])
                submission['price_rt_sell_unit'] = round(price_khr / KHR_TO_USD, 2)
                print(f"   💱 Converted: {price_khr} KHR → ${submission['price_rt_sell_unit']} USD")
            except:
                pass
        
        # Insert into database
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO price_monitoring (
            submission_time, survey_date, province, outlet_type,
            product_type, brand, sku, package_type,
            unit_per_ctn, price_ws_buy_ctn, price_ws_sell_ctn, price_rt_sell_unit
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            submission['submission_time'],
            submission['survey_date'],
            submission['province'],
            submission['outlet_type'],
            submission['product_type'],
            submission['brand'],
            submission['sku'],
            submission['package_type'],
            submission['unit_per_ctn'],
            submission['price_ws_buy_ctn'],
            submission['price_ws_sell_ctn'],
            submission['price_rt_sell_unit']
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"   ✅ Inserted: {submission['product_type']} - {submission['brand']}")
        
        return jsonify({"status": "success", "message": "Data inserted"}), 200
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "message": "API is running"}), 200

@app.route('/test', methods=['GET'])
def test():
    """Test endpoint"""
    return jsonify({
        "status": "ok",
        "message": "Webhook receiver is ready",
        "endpoints": {
            "/webhook": "POST - Receive Kobo submissions",
            "/health": "GET - Health check",
            "/test": "GET - Test endpoint"
        }
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print(f"🚀 Starting Kobo Webhook Receiver on port {port}")
    print(f"   Webhook URL: http://localhost:{port}/webhook")
    app.run(host='0.0.0.0', port=port, debug=False)
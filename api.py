"""
Flask API for Price Monitoring System
This API hosts your ETL script and handles webhooks from Supabase
"""
from flask import Flask, request, jsonify
import subprocess
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint - verifies API is running"""
    return jsonify({
        "status": "healthy",
        "message": "Price Monitoring API is running"
    }), 200

@app.route('/webhook', methods=['POST'])
def webhook():
    """
    Webhook endpoint that triggers your ETL process
    Called by Supabase when new data is inserted
    """
    try:
        # Get the webhook payload
        payload = request.json
        logger.info(f"Webhook received: {payload}")
        
        # Log the event type
        event_type = payload.get('type', 'unknown') if payload else 'unknown'
        logger.info(f"Processing {event_type} event")
        
        # Run your existing ETL script
        logger.info("Starting ETL script...")
        result = subprocess.run(
            ['python', 'run_pandas_etl.py'],
            capture_output=True,
            text=True,
            timeout=120  # 2 minute timeout
        )
        
        if result.returncode == 0:
            logger.info("✅ ETL script completed successfully")
            logger.debug(f"Output: {result.stdout}")
            return jsonify({
                "status": "success",
                "message": "ETL completed"
            }), 200
        else:
            logger.error(f"❌ ETL script failed: {result.stderr}")
            return jsonify({
                "status": "error",
                "message": result.stderr
            }), 500
            
    except subprocess.TimeoutExpired:
        logger.error("❌ ETL script timed out after 120 seconds")
        return jsonify({
            "status": "error",
            "message": "ETL script timeout"
        }), 500
    except Exception as e:
        logger.error(f"❌ Webhook error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/trigger-etl', methods=['POST'])
def trigger_etl():
    """
    Manual trigger endpoint - runs ETL immediately
    Useful for testing or manual updates
    """
    try:
        logger.info("Manual ETL trigger received")
        
        result = subprocess.run(
            ['python', 'run_pandas_etl.py'],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info("✅ Manual ETL completed successfully")
            return jsonify({
                "status": "success",
                "output": result.stdout
            }), 200
        else:
            logger.error(f"❌ Manual ETL failed: {result.stderr}")
            return jsonify({
                "status": "error",
                "error": result.stderr
            }), 500
            
    except Exception as e:
        logger.error(f"❌ Manual trigger error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/test', methods=['GET'])
def test():
    """Simple test endpoint"""
    return jsonify({
        "message": "API is working!",
        "endpoints": {
            "/health": "GET - Health check",
            "/test": "GET - This message",
            "/webhook": "POST - Webhook endpoint",
            "/trigger-etl": "POST - Manual ETL trigger"
        }
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting Price Monitoring API on port {port}")
    app.run(host='0.0.0.0', port=port, debug=False)
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

print("="*50)
print("DATABASE CONNECTION TEST")
print("="*50)

# Get credentials from .env
host = os.getenv('DB_HOST')
database = os.getenv('DB_NAME')
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
port = os.getenv('DB_PORT')

print(f"Host: {host}")
print(f"Database: {database}")
print(f"User: {user}")
print(f"Port: {port}")
print("-"*50)

try:
    # Try to connect
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port=port
    )
    print("✅ SUCCESS! Connected to Supabase!")
    
    # Test a simple query
    cur = conn.cursor()
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print(f"PostgreSQL version: {version[0]}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"❌ FAILED: {e}")
    
print("="*50)
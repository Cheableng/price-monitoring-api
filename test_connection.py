import os
from dotenv import load_dotenv
from database.db_connection import db

# Load environment variables
load_dotenv()

# Print connection details
print("="*50)
print("CONNECTION TEST")
print("="*50)
print(f"Host: {os.getenv('DB_HOST')}")
print(f"User: {os.getenv('DB_USER')}")
print(f"Port: {os.getenv('DB_PORT')}")
print(f"Database: {os.getenv('DB_NAME')}")
print("-"*50)

# Test connection
if db.test_connection():
    print("✅ SUCCESS! Connected to database")
else:
    print("❌ FAILED! Could not connect to database")
print("="*50)
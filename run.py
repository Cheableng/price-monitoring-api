import sys
print("Python path:", sys.path)

try:
    print("Importing api...")
    from api import app
    print("✓ api imported successfully")
    
    print("Importing waitress...")
    from waitress import serve
    print("✓ waitress imported successfully")
    
    print("Starting server on port 8080...")
    serve(app, host='0.0.0.0', port=8080)
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
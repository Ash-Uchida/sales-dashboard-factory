from databricks import sql
import os
from dotenv import load_dotenv

load_dotenv()

# Load credentials from environment variables
DATABRICKS_SERVER_HOSTNAME = os.getenv("DATABRICKS_SERVER_HOSTNAME")
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

print("Host set:", bool(DATABRICKS_SERVER_HOSTNAME))
print("HTTP path set:", bool(DATABRICKS_HTTP_PATH))
print("Token set:", bool(DATABRICKS_TOKEN))
try:
# Connect
    conn = sql.connect(
        server_hostname=DATABRICKS_SERVER_HOSTNAME,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN
    )
    cursor = conn.cursor()
        # Run a simple test query
    cursor.execute("SELECT 1 AS test")
    result = cursor.fetchall()
   
    if result[0][0] == 1:
        print("✅ Connection to Databricks successful!")
    else:
        print("⚠ Connected, but test query returned unexpected result:", result)
 
except Exception as e:
    print("❌ Connection failed:", e)
 
finally:
    # Close resources
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()
 
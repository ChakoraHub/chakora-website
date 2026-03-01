import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import  os
import sys
import time
import json

# Helper to always return a fresh DB connection with RSA key authentication
def get_db_connection():
    try:
        # Load RSA private key
        with open('rsa_key.p8', 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )

        # Convert private key to bytes
        pkb = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Connect to Snowflake using RSA key
        conn = snowflake.connector.connect(
            user='ChakoraHub',
            account='gpguymt-ta88699',
            private_key=pkb,
            warehouse='COMPUTE_WH',
            database='"VSRSUBHASH$CHAKORA_DB"',
            schema="CHAKORA"
        )
        print("✅ Connected to Snowflake using RSA key")
        return conn
        
    except Exception as e:
        print("❌ DB Connection Error:", e)
        return None
    
connection = get_db_connection()
cursor = connection.cursor()

print("=" * 60)
print("Columns in nrm_students:")
cursor.execute("DESCRIBE TABLE chakora.nrm_students")
for row in cursor.fetchall():
    print(f"  - {row[0]}")

print("\n" + "=" * 60)
print("Columns in nrm_users:")
cursor.execute("DESCRIBE TABLE chakora.nrm_users")
for row in cursor.fetchall():
    print(f"  - {row[0]}")

print("\n" + "=" * 60)
print("Columns in nrm_logins:")
cursor.execute("DESCRIBE TABLE chakora.nrm_logins")
for row in cursor.fetchall():
    print(f"  - {row[0]}")

print("\n" + "=" * 60)
print("Columns in nrm_registrations:")
cursor.execute("DESCRIBE TABLE chakora.nrm_registrations")
for row in cursor.fetchall():
    print(f"  - {row[0]}")

cursor.close()
connection.close()
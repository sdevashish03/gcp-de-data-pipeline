from google.oauth2 import service_account
import os

key_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
print("Using key_path:", key_path)

try:
    credentials = service_account.Credentials.from_service_account_file(key_path)
    print("Credentials loaded successfully.")
except Exception as e:
    print("Failed to load credentials:", e)

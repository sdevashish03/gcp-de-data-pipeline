import secrets
import base64

# Generate a 32-byte (256-bit) secure random key
key = secrets.token_bytes(32)

# Optionally encode it in base64 for readability/storage
encoded_key = base64.urlsafe_b64encode(key).decode('utf-8')

print("256-bit secret key:", encoded_key)
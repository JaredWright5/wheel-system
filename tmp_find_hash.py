from wheel.clients.schwab_client import SchwabClient
import json

s = SchwabClient.from_env()
accounts = s.get_accounts()

print("Full accounts response structure:")
print(json.dumps(accounts, indent=2))

print("\n" + "="*60)
print("Searching for any hash-related fields:")
print("="*60)

def find_hash_fields(obj, path=""):
    """Recursively search for any field containing 'hash'"""
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_path = f"{path}.{k}" if path else k
            if "hash" in k.lower():
                print(f"Found: {new_path} = {v}")
            find_hash_fields(v, new_path)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            find_hash_fields(item, f"{path}[{i}]")

find_hash_fields(accounts)


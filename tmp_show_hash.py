from wheel.clients.schwab_client import SchwabClient
import json

s = SchwabClient.from_env()
accounts = s.get_accounts()

print("Accounts response:")
print(json.dumps(accounts, indent=2))

print("\n" + "="*50)
print("Extracting hashValue:")
print("="*50)

# Handle different response shapes
if isinstance(accounts, list):
    for i, acc in enumerate(accounts):
        sec_account = acc.get("securitiesAccount", {})
        account_num = sec_account.get("accountNumber")
        hash_value = sec_account.get("hashValue")
        print(f"\nAccount {i+1}:")
        print(f"  accountNumber: {account_num}")
        print(f"  hashValue: {hash_value}")
elif isinstance(accounts, dict):
    items = accounts.get("accounts", [])
    for i, acc in enumerate(items):
        sec_account = acc.get("securitiesAccount", {})
        account_num = sec_account.get("accountNumber")
        hash_value = sec_account.get("hashValue")
        print(f"\nAccount {i+1}:")
        print(f"  accountNumber: {account_num}")
        print(f"  hashValue: {hash_value}")

# Test resolve_account_hash
print("\n" + "="*50)
print("Using resolve_account_hash():")
print("="*50)
hash_value = s.resolve_account_hash()
print(f"Resolved hashValue: {hash_value}")


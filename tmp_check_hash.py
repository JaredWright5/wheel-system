from wheel.clients.schwab_client import SchwabClient
import json

s = SchwabClient.from_env()
accounts = s.get_accounts()

# Extract account number from the response
if isinstance(accounts, list) and accounts:
    sec_account = accounts[0].get("securitiesAccount", {})
    account_num = sec_account.get("accountNumber")
    print(f"Account Number: {account_num}")
    
    # Try calling get_account with the account number
    print(f"\nCalling get_account('{account_num}', fields='positions')...")
    try:
        account_detail = s.get_account(account_num, fields="positions")
        print("\nAccount Detail Response:")
        print(json.dumps(account_detail, indent=2))
        
        # Check for hashValue in the response
        if isinstance(account_detail, dict):
            sec = account_detail.get("securitiesAccount", account_detail)
            hash_val = sec.get("hashValue")
            print(f"\nhashValue in account detail: {hash_val}")
    except Exception as e:
        print(f"Error calling get_account: {e}")
        import traceback
        traceback.print_exc()


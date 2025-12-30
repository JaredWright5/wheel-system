from wheel.clients.schwab_client import SchwabClient

s = SchwabClient.from_env()
print(s.get_accounts())


.PHONY: db-push db-smoke

db-push:
	supabase db push

db-smoke:
	PYTHONPATH=. python -m apps.worker.src.db_smoketest


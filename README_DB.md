# Database Migrations

This repo uses Supabase CLI for database schema management.

## Setup

After cloning the repo, link to your Supabase project:

```bash
supabase login
supabase link --project-ref <YOUR_PROJECT_REF>
```

This creates `supabase/config.toml` with your project configuration.

## Creating Migrations

Create new migration files in `supabase/migrations/` with this naming convention:

```
supabase/migrations/YYYYMMDDHHMMSS_description.sql
```

Example:
```bash
supabase/migrations/20241230180000_add_screening_picks_table.sql
```

## Applying Migrations

**Remote (Production/Staging):**

Push migrations to your linked Supabase project:

```bash
supabase db push
```

Or use the Makefile:

```bash
make db-push
```

This command:
- Applies all migrations in `supabase/migrations/` that haven't been applied yet
- Runs against your remote Supabase database
- Is idempotent (safe to run multiple times)

**Verifying Migrations:**

After running `supabase db push`, you can verify in Supabase Dashboard:
1. Go to Database → Migrations to see applied migrations
2. Go to Database → Tables to see created tables
3. Go to Database → Views to see created views

**Local (Development):**

Currently not used. We deploy directly to remote.

## Testing Database Connection

Test your database connection:

```bash
make db-smoke
```

Or directly:

```bash
PYTHONPATH=. python -m apps.worker.src.db_smoketest
```


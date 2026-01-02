# Database Documentation

This document describes the database schema, tables, views, migrations, and data relationships.

**Target Audience**: Database administrators, developers, LLM assistants

**Purpose**: Understand the database structure, relationships, and migration strategy.











## Overview

The system uses **Supabase** (hosted PostgreSQL) for data storage. Schema is managed via **Supabase CLI** with migration files in `supabase/migrations/`.

### Database Management
- **Migrations**: `supabase/migrations/YYYYMMDDHHMMSS_description.sql`
- **Apply Migrations**: `make db-push` or `supabase db push`
- **Test Connection**: `make db-smoke` or `PYTHONPATH=. python -m apps.worker.src.db_smoketest`











## Core Tables

### screening_runs
**Purpose**: Tracks each execution of the weekly screener or daily tracker.

**Key Columns**:
- `run_id` (UUID, PK) - Unique identifier for each run
- `run_ts` (timestamptz) - When the run started
- `status` (text) - 'running', 'success', or 'failed'
- `universe_size` (integer) - Number of tickers in universe
- `candidates_count` (integer) - Number of candidates generated
- `picks_count` (integer) - Number of picks generated
- `build_sha` (text) - Git commit SHA (for tracking deployments)
- `finished_at` (timestamptz) - When the run completed
- `error` (text) - Error message if failed (truncated to 800 chars)
- `notes` (text) - Human-readable notes

**Indexes**:
- `idx_screening_runs_run_ts_desc` - For run history queries
- `idx_screening_runs_status` - For filtering by status

**Lifecycle**:
1. Row created with `status='running'` at start
2. Updated with `status='success'` and `finished_at` on completion
3. Updated with `status='failed'` and `error` on failure

### screening_candidates
**Purpose**: Stores scored and ranked stock candidates from each screening run.

**Key Columns**:
- `id` (UUID, PK)
- `run_id` (UUID, FK → screening_runs) - Links to screening run
- `ticker` (text) - Stock symbol
- `score` (numeric) - Composite wheel score (0-100)
- `rank` (integer) - Rank within run (1 = highest score)
- `price` (numeric) - Current stock price
- `market_cap` (numeric) - Market capitalization
- `sector` (text) - Company sector
- `industry` (text) - Company industry
- `iv` (numeric) - Implied volatility (from Schwab, nullable)
- `iv_rank` (numeric) - IV rank (nullable)
- `beta` (numeric) - Stock beta
- `rsi` (numeric) - RSI technical indicator (from cache)
- `earn_in_days` (integer) - Days until earnings (nullable, TODO)
- `sentiment_score` (numeric) - Sentiment score (0-100)
- `metrics` (jsonb) - Full raw data dump (profile, quote, ratios, etc.)
- `created_at`, `updated_at` (timestamptz)

**Constraints**:
- Unique: `(run_id, ticker)` - One candidate per ticker per run

**Indexes**:
- `idx_screening_candidates_run_id` - For filtering by run
- `idx_screening_candidates_ticker` - For filtering by ticker
- `idx_screening_candidates_score_desc` - For sorting by score

### screening_picks
**Purpose**: Stores generated trading picks (CSP and CC options).

**Key Columns**:
- `id` (UUID, PK)
- `run_id` (UUID, FK → screening_runs) - Links to screening run
- `ticker` (text) - Stock symbol
- `action` (text) - 'CSP' or 'CC'
- `expiration` (date) - Option expiration date
- `dte` (integer) - Days to expiration
- `target_delta` (numeric) - Target delta for selection
- `strike` (numeric) - Option strike price
- `premium` (numeric) - Option premium (bid price)
- `annualized_yield` (numeric) - Calculated annualized yield
- `delta` (numeric) - Actual option delta
- `score`, `rank`, `price`, `beta`, `rsi`, etc. - Copied from candidate data
- `pick_metrics` (jsonb) - Additional pick-specific data
- `created_at`, `updated_at` (timestamptz)

**Constraints**:
- Unique: `(run_id, ticker, action)` - One pick per ticker per action per run

**Indexes**:
- `idx_screening_picks_run_id` - For filtering by run
- `idx_screening_picks_action` - For filtering by action (CSP vs CC)
- `idx_screening_picks_annualized_yield_desc` - For sorting by yield

### rsi_snapshots
**Purpose**: Caches RSI values to avoid API rate limits.

**Key Columns**:
- `id` (UUID, PK)
- `ticker` (text) - Stock symbol
- `as_of_date` (date) - Date of snapshot
- `interval` (text) - RSI interval ('daily', 'weekly', etc.)
- `period` (integer) - RSI period (default: 14)
- `rsi` (numeric) - RSI value (nullable if fetch failed)
- `source` (text) - 'fmp' (source of data)
- `created_at` (timestamptz)

**Constraints**:
- Unique: `(ticker, as_of_date, interval, period)` - One snapshot per ticker per day

**Indexes**:
- `idx_rsi_snapshots_ticker` - For filtering by ticker
- `idx_rsi_snapshots_as_of_date_desc` - For finding latest snapshot
- `idx_rsi_snapshots_ticker_date` - Composite index for lookups

**Usage**:
- Populated daily by `rsi_snapshot.py` worker
- Read by `weekly_screener.py` (avoids real-time API calls)

### account_snapshots
**Purpose**: Historical snapshots of account balances.

**Key Columns**:
- `id` (UUID, PK)
- `run_id` (UUID, FK → screening_runs) - Links to tracker run
- `run_ts` (timestamptz) - Snapshot timestamp
- `account_hash` (text) - Schwab account hashValue
- `net_liquidation` (numeric) - Total account value
- `cash` (numeric) - Cash balance
- `buying_power` (numeric) - Available buying power
- `maintenance_requirement` (numeric) - Maintenance margin
- `raw` (jsonb) - Full account response from Schwab
- `created_at` (timestamptz)

**Usage**:
- Populated daily by `daily_tracker.py` worker
- Used for tracking account value over time

### position_snapshots
**Purpose**: Historical snapshots of account positions.

**Key Columns**:
- `id` (UUID, PK)
- `run_id` (UUID, FK → screening_runs) - Links to tracker run
- `run_ts` (timestamptz) - Snapshot timestamp
- `account_hash` (text) - Schwab account hashValue
- `symbol` (text) - Stock symbol
- `asset_type` (text) - 'EQUITY', 'STOCK', etc.
- `quantity` (numeric) - Position quantity
- `average_price` (numeric) - Average cost basis
- `market_value` (numeric) - Current market value
- `day_pnl` (numeric) - Daily profit/loss
- `total_pnl` (numeric) - Total profit/loss (placeholder)
- `raw` (jsonb) - Full position response from Schwab
- `created_at` (timestamptz)

**Usage**:
- Populated daily by `daily_tracker.py` worker
- Used for tracking positions over time
- Used by `build_cc_picks.py` for covered call generation

### tickers
**Purpose**: Master reference table for stock tickers.

**Key Columns**:
- `ticker` (text, PK) - Stock symbol
- `name` (text) - Company name
- `exchange` (text) - Exchange (NYSE, NASDAQ, etc.)
- `sector` (text) - Company sector
- `industry` (text) - Company industry
- `market_cap` (numeric) - Market capitalization
- `currency` (text) - Currency (default: 'USD')
- `is_active` (boolean) - Whether ticker is active
- `updated_at` (timestamptz)

**Usage**:
- Populated by `weekly_screener.py` (upsert)
- Used for reference data and joins

### approved_universe
**Purpose**: Tracks top 40 candidates for stability week-to-week.

**Key Columns**:
- `ticker` (text, PK) - Stock symbol
- `approved` (boolean) - Whether ticker is approved
- `last_run_id` (UUID) - Last run that included this ticker
- `last_run_ts` (timestamptz) - When last included
- `last_rank` (integer) - Rank in last run
- `last_score` (integer) - Score in last run
- `notes` (text) - Optional notes
- `updated_at` (timestamptz)

**Usage**:
- Populated by `weekly_screener.py` (top 40 candidates)
- Used for maintaining stable universe across runs











## Database Views

Views are created for dashboard queries (read-only, optimized).

### v_run_history
**Purpose**: Run history summary for dashboard.

**Columns**: `run_id`, `run_ts`, `status`, `universe_size`, `candidates_count`, `picks_count`, `build_sha`, `notes`

**Ordering**: `run_ts DESC` (latest first)

**Limit**: 200 runs

### v_latest_run_top25_candidates
**Purpose**: Top 25 candidates from latest successful run.

**Columns**: All `screening_candidates` columns

**Filtering**: Latest `run_id` with `status='success'`

**Ordering**: `score DESC` (highest first)

**Limit**: 25 candidates

### v_latest_run_csp_picks
**Purpose**: CSP picks from latest run.

**Columns**: All `screening_picks` columns

**Filtering**: Latest `run_id` AND `action='CSP'`

**Ordering**: `annualized_yield DESC` (highest yield first)

### v_latest_run_cc_picks
**Purpose**: CC picks from latest run.

**Columns**: All `screening_picks` columns

**Filtering**: Latest `run_id` AND `action='CC'`

**Ordering**: `annualized_yield DESC` (highest yield first)

### v_latest_run_all_picks
**Purpose**: Combined CSP and CC picks from latest run.

**Columns**: All `screening_picks` columns

**Filtering**: Latest `run_id`

**Ordering**: 
1. `action` (CSP first, then CC)
2. `annualized_yield DESC` (highest yield first)











## Migrations

### Migration Strategy
- **Location**: `supabase/migrations/`
- **Naming**: `YYYYMMDDHHMMSS_description.sql`
- **Idempotent**: All migrations use `IF NOT EXISTS` or `CREATE OR REPLACE`
- **Applied**: Via `supabase db push` (remote only, no local dev)

### Migration Files

#### 20251231134029_screening_tables_and_views.sql
- Creates `screening_candidates` table
- Creates `screening_picks` table
- Creates initial dashboard views
- Adds missing columns if tables already exist (backward compatibility)

#### 20251231145651_screening_runs_status.sql
- Adds lifecycle tracking columns to `screening_runs`
- `status`, `error`, `candidates_count`, `picks_count`, `build_sha`, `finished_at`
- Adds status constraint and indexes

#### 20251231154829_screening_picks_csp_fields.sql
- Ensures all CSP-related columns exist in `screening_picks`
- `expiration`, `delta`, `target_delta`, `dte`, `strike`, `premium`, `annualized_yield`, `pick_metrics`

#### 20251231164430_latest_run_all_picks_view.sql
- Creates `v_latest_run_all_picks` view
- Combines CSP and CC picks with proper ordering

#### 20251231170000_dashboard_v1_views.sql
- Creates/replaces all dashboard views
- Adds helpful indexes
- Uses `DROP VIEW IF EXISTS` to handle view changes

#### 20260101120000_rsi_snapshots.sql
- Creates `rsi_snapshots` table
- Adds indexes for efficient lookups











## Data Relationships

### Run Lifecycle
```
screening_runs (1) ──< (many) screening_candidates
screening_runs (1) ──< (many) screening_picks
screening_runs (1) ──< (many) account_snapshots
screening_runs (1) ──< (many) position_snapshots
```

### Candidate to Pick
```
screening_candidates (1) ──< (many) screening_picks
  (via run_id + ticker, not formal FK)
```

### RSI Cache
```
rsi_snapshots (independent) ──> (read by) weekly_screener
  (no formal FK, referenced by ticker)
```











## Indexes

### Performance Indexes
- Run-based queries: `idx_*_run_id` on all tables with `run_id`
- Score/yield sorting: `idx_*_score_desc`, `idx_*_annualized_yield_desc`
- Ticker lookups: `idx_*_ticker` on relevant tables
- Date-based queries: `idx_*_as_of_date_desc`, `idx_*_run_ts_desc`

### Composite Indexes
- `idx_rsi_snapshots_ticker_date` - For efficient RSI lookups
- `idx_screening_candidates_run_id` + `idx_screening_candidates_score_desc` - For run queries with sorting











## Data Types

### UUIDs
- All primary keys use `uuid` type
- Generated via `gen_random_uuid()` (requires `pgcrypto` extension)
- Used for: `run_id`, `id` columns

### Timestamps
- All timestamps use `timestamptz` (timezone-aware)
- Default: `now()` (current timestamp)
- Stored in UTC

### JSONB
- Used for: `metrics`, `pick_metrics`, `raw` columns
- Stores full API responses for debugging
- Allows flexible schema (no strict structure)

### Numeric
- Used for: prices, scores, yields, ratios
- No precision specified (PostgreSQL handles)
- Allows NULL values (missing data)











## Constraints

### Unique Constraints
- `screening_candidates`: `(run_id, ticker)` - One candidate per ticker per run
- `screening_picks`: `(run_id, ticker, action)` - One pick per ticker per action per run
- `rsi_snapshots`: `(ticker, as_of_date, interval, period)` - One snapshot per ticker per day
- `tickers`: `ticker` (PK) - One row per ticker
- `approved_universe`: `ticker` (PK) - One row per ticker

### Foreign Keys
- `screening_candidates.run_id` → `screening_runs.run_id` (CASCADE DELETE)
- `screening_picks.run_id` → `screening_runs.run_id` (CASCADE DELETE)
- `account_snapshots.run_id` → `screening_runs.run_id` (CASCADE DELETE)
- `position_snapshots.run_id` → `screening_runs.run_id` (CASCADE DELETE)

### Check Constraints
- `screening_runs.status` IN ('running', 'success', 'failed')











## Migration Best Practices

### Idempotent Migrations
- Always use `IF NOT EXISTS` for tables
- Use `CREATE OR REPLACE VIEW` for views
- Use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for columns
- Safe to run multiple times

### Backward Compatibility
- If tables exist before migration, add missing columns
- Use `DO $$ ... END $$` blocks for conditional column addition
- Check `information_schema` before altering

### View Updates
- Use `DROP VIEW IF EXISTS` before `CREATE VIEW` if column order changes
- Prevents "cannot change name of view column" errors











## Common Queries

### Get Latest Run
```sql
SELECT run_id FROM screening_runs
ORDER BY run_ts DESC LIMIT 1;
```

### Get Top Candidates
```sql
SELECT * FROM screening_candidates
WHERE run_id = (SELECT run_id FROM screening_runs ORDER BY run_ts DESC LIMIT 1)
ORDER BY score DESC LIMIT 25;
```

### Get Latest RSI
```sql
SELECT rsi FROM rsi_snapshots
WHERE ticker = 'AAPL'
  AND as_of_date = CURRENT_DATE
  AND interval = 'daily'
  AND period = 14;
```











## Related Files

- `supabase/migrations/*.sql` - Migration files
- `wheel/clients/supabase_client.py` - Database client
- `apps/worker/src/db_smoketest.py` - Connection test
- `README_DB.md` - Migration instructions
- `Makefile` - `db-push` and `db-smoke` targets


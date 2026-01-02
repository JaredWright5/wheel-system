# System Overview

This document provides a high-level understanding of the Wheel System architecture, major components, and how they interact.

**Target Audience**: Project managers, new developers, LLM assistants

**Purpose**: Get familiar with the codebase structure and major logic flows without diving into code details.











## System Architecture

The Wheel System is a **Python-based options trading automation platform** that runs on Render.com and uses Supabase for data storage.

### High-Level Flow

```
1. Weekly Screening (Monday 4:30 AM PT)
   └─> Fetches stock universe
   └─> Enriches with fundamentals, sentiment, RSI
   └─> Scores and ranks candidates
   └─> Stores in screening_candidates table

2. RSI Snapshot (Mon-Fri 4:30 AM PT)
   └─> Fetches RSI for all universe tickers
   └─> Caches in rsi_snapshots table
   └─> Used by weekly screener (avoids rate limits)

3. Pick Generation (Monday 4:30 AM PT)
   └─> CSP Picks: From top candidates
   └─> CC Picks: From existing positions
   └─> Stores in screening_picks table

4. Daily Tracking (Weekdays 4:30 AM PT)
   └─> Snapshots Schwab account balances
   └─> Tracks positions and market values
   └─> Stores in account_snapshots, position_snapshots

5. Dashboard (Web Service)
   └─> Displays latest screening results
   └─> Shows candidates, picks, run history
   └─> Read-only views from Supabase
```











## Major Components

### 1. Worker Scripts (`apps/worker/src/`)

**weekly_screener.py** - The core screening engine
- Loads universe (CSV or FMP)
- Fetches data: profile, quote, ratios, metrics, news, RSI
- Applies filters: price, market cap, RSI gates
- Scores candidates: fundamentals (50%), sentiment (20%), trend (20%), technical (10%)
- Writes to `screening_candidates` and `approved_universe` tables

**rsi_snapshot.py** - RSI data caching worker
- Processes all universe tickers daily
- Fetches RSI from FMP API
- Caches in `rsi_snapshots` table
- Prevents rate limiting during screening

**build_csp_picks.py** - Cash-Secured Put pick generator
- Loads top candidates from latest screening run
- Fetches Schwab option chains (PUT options)
- Selects optimal strikes using tiered expiration strategy
- Calculates annualized yield
- Writes to `screening_picks` table (action='CSP')

**build_cc_picks.py** - Covered Call pick generator
- Fetches eligible positions from Schwab (long, quantity >= 100)
- Fetches Schwab option chains (CALL options)
- Selects OTM calls in delta band [0.20, 0.30]
- Includes ex-dividend guardrail
- Writes to `screening_picks` table (action='CC')
- Supports test mode via `CC_TEST_TICKERS` env var

**daily_tracker.py** - Account snapshot worker
- Fetches account balances and positions from Schwab
- Creates snapshots in `account_snapshots` and `position_snapshots`
- Links to `screening_runs` table for tracking

### 2. API Clients (`wheel/clients/`)

**fmp_stable_client.py** - Financial Modeling Prep API
- Company screener, profile, quote, ratios, metrics
- Technical indicators (RSI)
- Stock news for sentiment analysis
- Uses stable endpoints (`/stable/` base URL)
- Includes retry logic and error handling

**schwab_client.py** - Schwab Trader API
- OAuth 2.0 token refresh
- Account data, positions, orders, transactions
- Handles account hash resolution automatically
- Read-only operations

**schwab_marketdata_client.py** - Schwab Market Data API
- Option chain fetching
- Separate from Trader API (different auth flow)

**supabase_client.py** - Database operations
- `insert_row()`, `upsert_rows()`, `update_rows()`
- `select_all()` for dashboard views
- Handles composite key deduplication
- Error handling and logging

### 3. Database (`supabase/migrations/`)

**Core Tables**:
- `screening_runs` - Tracks each screening execution (status, timestamps, counts)
- `screening_candidates` - Scored and ranked stocks per run
- `screening_picks` - Generated trading picks (CSP/CC)
- `rsi_snapshots` - Cached RSI values (daily snapshots)
- `account_snapshots` - Historical account balances
- `position_snapshots` - Historical position data
- `tickers` - Master ticker reference data
- `approved_universe` - Top 40 candidates for stability

**Views** (for dashboard):
- `v_run_history` - Run history summary
- `v_latest_run_top25_candidates` - Top 25 candidates
- `v_latest_run_csp_picks` - CSP picks from latest run
- `v_latest_run_cc_picks` - CC picks from latest run
- `v_latest_run_all_picks` - Combined CSP + CC picks

### 4. Dashboard (`apps/dashboard/`)

**FastAPI Web Application**:
- Routes: `/`, `/runs`, `/candidates`, `/picks`, `/health`
- Reads from Supabase views (read-only)
- Jinja2 templates for HTML rendering
- Static CSS for styling
- Error handling for missing views











## Data Flow

### Weekly Screening Flow

```
Monday 4:30 AM PT:
1. weekly_screener.py starts
2. Creates screening_runs row (status='running')
3. Loads universe (CSV or FMP company screener)
4. For each ticker:
   a. Fetches profile, quote, ratios, metrics from FMP
   b. Fetches news from FMP (for sentiment)
   c. Reads RSI from rsi_snapshots cache
   d. Applies filters (price, market cap, RSI)
   e. Calculates scores (fundamentals, sentiment, trend, technical)
5. Sorts by wheel_score (descending)
6. Writes to screening_candidates table
7. Updates approved_universe (top 40)
8. Updates screening_runs (status='success', counts, finished_at)
```

### Pick Generation Flow

```
Monday 4:30 AM PT (after screening):
1. build_csp_picks.py starts
   a. Loads latest run_id
   b. Fetches top candidates from screening_candidates
   c. For each candidate:
      - Fetches Schwab option chain (PUTs)
      - Selects expiration using tiered strategy
      - Selects strike based on target delta
      - Calculates annualized yield
   d. Deletes existing CSP picks for run_id
   e. Inserts new picks into screening_picks

2. build_cc_picks.py starts
   a. Loads latest run_id
   b. Fetches positions from Schwab (long, quantity >= 100)
   c. For each position:
      - Fetches Schwab option chain (CALLs)
      - Selects expiration using tiered strategy
      - Selects OTM call in delta band [0.20, 0.30]
      - Checks ex-dividend guardrail
   d. Deletes existing CC picks for run_id
   e. Inserts new picks into screening_picks
```

### RSI Caching Flow

```
Mon-Fri 4:30 AM PT:
1. rsi_snapshot.py starts
2. Loads universe (same as weekly_screener)
3. Checks existing cache for today
4. For each uncached ticker:
   a. Fetches RSI from FMP technical-indicators/rsi endpoint
   b. Stores in rsi_snapshots table
5. Weekly screener reads from cache (not real-time API calls)
```











## Key Design Decisions

### 1. Caching Strategy
- **RSI is cached daily** to avoid API rate limits
- Weekly screener reads from cache (fast, no rate limits)
- Cache is populated separately by `rsi_snapshot.py` worker

### 2. Run Lifecycle Tracking
- Each screening run has a `screening_runs` row
- Status: 'running' → 'success' or 'failed'
- Tracks: `candidates_count`, `picks_count`, `build_sha`, `finished_at`, `error`
- Enables debugging and monitoring

### 3. Tiered Expiration Selection
- Primary window: `MIN_DTE` to `MAX_DTE` (default 4-10 days)
- Fallback 1: `MIN_DTE` to `FALLBACK_MAX_DTE_1` (default 4-14 days)
- Fallback 2: `FALLBACK_MIN_DTE_2` to `FALLBACK_MAX_DTE_2` (default 1-21 days)
- Prevents skipping tickers due to expiration availability

### 4. Composite Key Deduplication
- `upsert_rows()` deduplicates in-Python before sending to Supabase
- Prevents Postgres error 21000 (duplicate key in same batch)
- Used for: `screening_candidates` (run_id, ticker), `screening_picks` (run_id, ticker, action)

### 5. Timezone Handling
- All timestamps use UTC (`datetime.now(timezone.utc)`)
- Render cron schedules are in UTC (12:30 UTC = 4:30 AM PT during PST)
- Note: Schedule doesn't adjust for DST (manual update needed)











## External Dependencies

### Financial Modeling Prep (FMP)
- **Purpose**: Market data, fundamentals, technical indicators
- **Endpoints Used**: company-screener, profile, quote, ratios-ttm, key-metrics-ttm, technical-indicators/rsi, stock-news
- **Rate Limits**: 300 calls/minute (Starter plan)
- **Client**: `wheel/clients/fmp_stable_client.py`

### Schwab API
- **Trader API**: Account data, positions, orders
- **Market Data API**: Option chains
- **Auth**: OAuth 2.0 refresh token flow
- **Clients**: `wheel/clients/schwab_client.py`, `wheel/clients/schwab_marketdata_client.py`

### Supabase
- **Purpose**: PostgreSQL database (hosted)
- **Migrations**: Managed via Supabase CLI
- **Client**: `wheel/clients/supabase_client.py`











## File Organization

### Package Structure
- **`wheel/`** - Top-level package (stable imports)
- **`apps/`** - Application code (workers, dashboard)
- **`supabase/migrations/`** - Database schema migrations
- **`data/`** - Static data files (universe CSV)

### Import Pattern
All imports use `wheel.` prefix:
```python
from wheel.clients.fmp_stable_client import FMPStableClient
from wheel.clients.supabase_client import insert_row, upsert_rows
```

This ensures stable imports across the codebase and works with Render's `PYTHONPATH=/opt/render/project/src`.











## Next Steps

For detailed information on specific areas, see:
- **authFlow.md** - Schwab OAuth authentication
- **dataFlow.md** - Detailed screening and pick generation workflows
- **apiClients.md** - API client implementations and quirks
- **database.md** - Schema details and migration strategy
- **deployment.md** - Render configuration and cron schedules
- **dashboard.md** - Web application structure
- **issues.md** - Known issues and technical debt


# Data Flow Documentation

This document explains the detailed data flows for screening, pick generation, and tracking processes.

**Target Audience**: Project managers, developers, LLM assistants

**Purpose**: Understand step-by-step how data moves through the system, from universe loading to final pick generation.











## Weekly Screening Flow

### Entry Point
- **File**: `apps/worker/src/weekly_screener.py`
- **Schedule**: Monday 4:30 AM PT (12:30 UTC during PST)
- **Trigger**: Render cron job (`wheel-weekly-screener`)

### Step-by-Step Process

#### 1. Initialization
- Loads environment variables from `.env.local`
- Initializes FMP Stable client
- Reads `BUILD_SHA` from `RENDER_GIT_COMMIT` (for tracking)
- Determines universe source (`UNIVERSE_SOURCE` env var, default: "csv")

#### 2. Universe Loading
**Option A: CSV Source** (default)
- Reads from `data/universe_us.csv`
- Simple CSV parsing (symbol, name, exchange)
- Returns list of ~168 tickers

**Option B: FMP Stable Source**
- Calls FMP company screener for NYSE, NASDAQ, AMEX
- Applies filters: min price, min market cap, min avg volume
- Limits to 500 companies per exchange (to avoid timeouts)
- Deduplicates and filters
- Returns list of eligible companies

#### 3. Run Creation
- Inserts row into `screening_runs` table
- Status: `'running'`
- Stores: `run_ts`, `universe_size`, `build_sha`, `notes`
- Returns `run_id` (UUID) for linking all subsequent data

#### 4. Candidate Processing Loop
For each ticker in universe:

**a. Data Fetching** (from FMP):
- `profile()` - Company profile (name, sector, industry, beta, market cap)
- `quote()` - Current price, 52-week high/low
- `ratios_ttm()` - Financial ratios (profit margins, ROE, P/E, debt/equity)
- `key_metrics_ttm()` - Key metrics (additional financial data)
- `stock_news()` - Recent news articles (for sentiment analysis)
- `get_rsi_from_cache()` - RSI from Supabase cache (not real-time API call)

**b. Filtering Gates**:
- Price required (must have valid price)
- Market cap >= `MIN_MARKET_CAP` (default: $2B)
- Price >= `MIN_PRICE` (default: $5.0)
- RSI gate (optional): `RSI_MIN` <= RSI <= `RSI_MAX` (default: 30-70)
- Missing RSI doesn't filter out (treated as neutral)

**c. Scoring**:
- **Fundamentals Score** (50% weight):
  - Profitability: Net profit margin, operating profit margin, ROE
  - Valuation: P/E ratio (prefer < 25)
  - Leverage: Debt/equity ratio (prefer < 1.0)
- **Sentiment Score** (20% weight):
  - Analyzes news headlines for positive/negative keywords
  - Normalized to [-1, 1] then converted to [0, 100]
- **Trend Score** (20% weight):
  - Based on 52-week price position
  - Prefers middle of range (avoids extremes)
- **Technical Score** (10% weight):
  - Based on RSI value
  - Prefers RSI in range 30-70
  - Missing RSI = neutral (50 points)

**d. Composite Score**:
- `wheel_score = 0.50 * fundamentals + 0.20 * sentiment + 0.20 * trend + 0.10 * technical`
- Clamped to [0, 100]

**e. Data Storage**:
- Creates `Candidate` dataclass object
- Appends to candidates list
- Also creates `ticker` row for master reference table

#### 5. Final Processing
- Sorts candidates by `wheel_score` (descending)
- Writes to `screening_candidates` table (with rank)
- Updates `tickers` table (upsert)
- Updates `approved_universe` table (top 40 candidates)
- Updates `screening_runs` row:
  - Status: `'success'`
  - `candidates_count`: Number of candidates
  - `picks_count`: 0 (picks generated separately)
  - `finished_at`: Completion timestamp

#### 6. Error Handling
- If exception occurs:
  - Updates `screening_runs` row:
    - Status: `'failed'`
    - `error`: Error message (truncated to 800 chars)
    - `finished_at`: Failure timestamp
  - Logs full exception
  - Re-raises exception (fails the cron job)

### Key Data Structures

**Candidate Dataclass**:
```python
@dataclass
class Candidate:
    ticker: str
    name: str
    sector: Optional[str]
    industry: Optional[str]
    market_cap: Optional[int]
    price: Optional[float]
    beta: Optional[float]
    rsi: Optional[float]
    fundamentals_score: int
    sentiment_score: int
    trend_score: int
    technical_score: int
    wheel_score: int
    reasons: Dict[str, Any]
    features: Dict[str, Any]  # Full raw data dump
```











## RSI Snapshot Flow

### Entry Point
- **File**: `apps/worker/src/rsi_snapshot.py`
- **Schedule**: Mon-Fri 4:30 AM PT (12:30 UTC during PST)
- **Trigger**: Render cron job (`wheel-rsi-snapshot`)

### Step-by-Step Process

#### 1. Initialization
- Initializes FMP Stable client
- Loads universe (same logic as weekly screener)
- Gets today's date (UTC)

#### 2. Cache Check
- Queries `rsi_snapshots` table for today's data
- Filters by: `as_of_date = today`, `interval`, `period`
- Creates set of already-cached tickers
- Skips cached tickers (idempotent)

#### 3. RSI Fetching Loop
For each uncached ticker:
- Calls `fmp.technical_indicator_rsi(ticker, period=14, interval='daily')`
- FMP endpoint: `/stable/technical-indicators/rsi?symbol=...&periodLength=14&timeframe=1day`
- Extracts latest RSI value from response (first item in array)
- Creates row for `rsi_snapshots` table:
  - `ticker`, `as_of_date`, `interval`, `period`, `rsi`, `source='fmp'`

#### 4. Batch Insert
- Upserts rows in batches of 50 (to avoid large transactions)
- Uses composite key: `(ticker, as_of_date, interval, period)`
- Handles errors gracefully (logs and continues)

#### 5. Summary Logging
- Logs: `fetched_ok`, `fetched_missing`, `skipped_due_to_cache`, `inserted`

### Why Caching?
- **Problem**: FMP API has rate limits (300 calls/minute)
- **Solution**: Fetch RSI once per day, cache in database
- **Benefit**: Weekly screener reads from cache (fast, no rate limits)
- **Trade-off**: RSI data is up to 24 hours old (acceptable for weekly screening)











## CSP Pick Generation Flow

### Entry Point
- **File**: `apps/worker/src/build_csp_picks.py`
- **Schedule**: Monday 4:30 AM PT (after weekly screener)
- **Trigger**: Render cron job (`wheel-build-csp-picks`)

### Step-by-Step Process

#### 1. Initialization
- Loads latest `run_id` from `screening_runs` (or uses `RUN_ID` env var override)
- Initializes Schwab Market Data client
- Loads configuration: `PICKS_N`, `MIN_DTE`, `MAX_DTE`, fallback windows

#### 2. Candidate Loading
- Queries `screening_candidates` table for latest run
- Filters by: `run_id = latest_run_id`
- Orders by: `score DESC`
- Limits to: `PICKS_N` (default: 25)

#### 3. Pick Generation Loop
For each candidate:

**a. Option Chain Fetching**:
- Calls `schwab_marketdata.get_option_chain(ticker, contract_type='PUT', strike_count=80)`
- Parses expiration dates from chain response
- Extracts PUT options for each expiration

**b. Expiration Selection** (Tiered Strategy):
- **Primary**: Finds expiration in window `[MIN_DTE, MAX_DTE]` (default: 4-10 days)
- **Fallback 1**: If not found, tries `[MIN_DTE, FALLBACK_MAX_DTE_1]` (default: 4-14 days)
- **Fallback 2**: If still not found, tries `[FALLBACK_MIN_DTE_2, FALLBACK_MAX_DTE_2]` (default: 1-21 days)
- Logs which window was used

**c. Strike Selection**:
- Filters PUTs by: `bid > 0` (must have liquidity)
- Selects PUT closest to target delta (typically 0.30 for CSP)
- Extracts: `strike`, `premium` (bid), `delta`

**d. Yield Calculation**:
- `annualized_yield = (premium / strike) * (365 / dte) * 100`
- Stores in pick row

**e. Data Storage**:
- Creates row for `screening_picks` table:
  - `run_id`, `ticker`, `action='CSP'`
  - `expiration`, `dte`, `strike`, `premium`, `delta`, `annualized_yield`
  - Copies candidate data: `score`, `rank`, `price`, `beta`, `rsi`, etc.

#### 4. Final Processing
- Deletes existing CSP picks for `run_id` (idempotent)
- Inserts new picks (upsert with composite key: `run_id`, `ticker`, `action`)
- Logs summary: `processed`, `created`, `skipped_*` counters

### Error Handling
- Individual ticker failures don't stop the run
- Logs warnings for skipped tickers (no chain, no expiration, no deltas)
- Continues processing remaining candidates











## CC Pick Generation Flow

### Entry Point
- **File**: `apps/worker/src/build_cc_picks.py`
- **Schedule**: Monday 4:30 AM PT (after weekly screener)
- **Trigger**: Render cron job (`wheel-build-cc-picks`) - Note: Not in render.yaml yet

### Step-by-Step Process

#### 1. Initialization
- Loads latest `run_id` (or `RUN_ID` env var override)
- Initializes Schwab Trader API client
- Loads configuration: `CC_PICKS_N`, DTE windows, delta band, ex-dividend guardrail

#### 2. Position Loading
**Option A: Test Mode** (`CC_TEST_TICKERS` env var set):
- Creates synthetic positions for specified tickers
- Quantity: 100 (minimum for covered calls)
- Fetches current price from FMP for OTM check

**Option B: Real Positions**:
- Calls `schwab.get_account(fields='positions')`
- Filters positions:
  - Asset type: `EQUITY` or `STOCK`
  - Long position: `longQuantity > 0`
  - Quantity >= 100 (minimum for covered calls)

#### 3. Pick Generation Loop
For each eligible position:

**a. Option Chain Fetching**:
- Calls `schwab_marketdata.get_option_chain(ticker, contract_type='CALL', strike_count=80)`
- Parses expiration dates

**b. Expiration Selection**:
- Uses same tiered strategy as CSP picks
- Primary: `[MIN_DTE, MAX_DTE]`
- Fallback windows if needed

**c. Strike Selection**:
- Filters CALLs by:
  - `bid > 0` (liquidity)
  - `delta` in range `[DELTA_MIN, DELTA_MAX]` (default: 0.20-0.30)
  - OTM: `strike > current_price` (out-of-the-money)
- Selects best option (highest premium in delta band)

**d. Ex-Dividend Guardrail**:
- Checks if ex-dividend date is within `EXDIV_SKIP_DAYS` (default: 2 days)
- Skips ticker if ex-dividend too close (avoids early assignment risk)

**e. Data Storage**:
- Creates row for `screening_picks` table:
  - `run_id`, `ticker`, `action='CC'`
  - `expiration`, `dte`, `strike`, `premium`, `delta`, `annualized_yield`
  - Copies candidate data if available from `screening_candidates`

#### 4. Final Processing
- Deletes existing CC picks for `run_id`
- Inserts new picks
- Logs summary with counters











## Daily Tracker Flow

### Entry Point
- **File**: `apps/worker/src/daily_tracker.py`
- **Schedule**: Weekdays 4:30 AM PT (12:30 UTC during PST)
- **Trigger**: Render cron job (`wheel-daily-tracker`)

### Step-by-Step Process

#### 1. Initialization
- Initializes Schwab Trader API client
- Gets current timestamp (UTC)

#### 2. Run Creation
- Inserts row into `screening_runs` table
- `notes='DAILY_TRACKER'` (identifies tracker runs)
- Returns `run_id` for linking

#### 3. Account Snapshot
- Calls `schwab.get_account(fields='positions')`
- Extracts account balances:
  - `net_liquidation` (total account value)
  - `cash` (cash balance)
  - `buying_power`
  - `maintenance_requirement`
- Resolves account hash (for multi-account scenarios)
- Inserts row into `account_snapshots` table

#### 4. Position Snapshot
- Extracts positions from account response
- For each position:
  - Extracts: `symbol`, `asset_type`, `quantity`, `average_price`, `market_value`, `day_pnl`
  - Creates row for `position_snapshots` table
- Deduplicates by `(run_id, symbol)` (handles duplicate positions)
- Upserts to `position_snapshots` table

#### 5. Summary
- Returns summary dict with `run_id`, `account_hash`, `positions` count











## Data Dependencies

### Screening → Picks
- CSP picks depend on `screening_candidates` (latest run)
- CC picks depend on Schwab positions (real-time)

### RSI Cache → Screening
- Weekly screener reads from `rsi_snapshots` cache
- Cache populated by `rsi_snapshot.py` worker (daily)

### Run Lifecycle
- All data linked via `run_id` (UUID)
- `screening_runs` table tracks status and metadata
- Cascade deletes: Deleting run deletes candidates and picks











## Performance Considerations

### API Rate Limits
- FMP: 300 calls/minute (Starter plan)
- Schwab: Varies by endpoint (not documented)
- RSI caching prevents rate limit issues during screening

### Batch Processing
- RSI snapshots: Batches of 50 rows
- Candidate processing: Sequential (one ticker at a time)
- Pick generation: Sequential (one ticker at a time)

### Time Complexity
- Weekly screener: O(n) where n = universe size (~168 tickers)
- RSI snapshot: O(n) where n = universe size
- Pick generation: O(m) where m = number of candidates/picks (default: 25)











## Error Recovery

### Partial Failures
- Individual ticker failures don't stop the run
- Failed tickers are logged and skipped
- Successful tickers are still processed and stored

### Run Status Tracking
- `screening_runs.status` field tracks: 'running', 'success', 'failed'
- Failed runs can be identified and debugged
- `error` field stores truncated error message

### Idempotency
- RSI snapshots: Skips already-cached tickers
- Pick generation: Deletes existing picks before inserting (clean state)
- Run creation: Each run gets unique `run_id` (UUID)











## Related Files

- `apps/worker/src/weekly_screener.py` - Main screening logic
- `apps/worker/src/rsi_snapshot.py` - RSI caching
- `apps/worker/src/build_csp_picks.py` - CSP pick generation
- `apps/worker/src/build_cc_picks.py` - CC pick generation
- `apps/worker/src/daily_tracker.py` - Account tracking
- `wheel/clients/fmp_stable_client.py` - FMP API client
- `wheel/clients/schwab_marketdata_client.py` - Option chain fetching
- `wheel/clients/supabase_client.py` - Database operations


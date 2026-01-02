# API Clients Documentation

This document describes the external API integrations, their implementations, quirks, and workarounds.

**Target Audience**: Developers working on API integrations, LLM assistants

**Purpose**: Understand API client implementations, error handling, rate limits, and any special considerations.











## Overview

The system integrates with three main external APIs:
1. **Financial Modeling Prep (FMP)** - Market data and fundamentals
2. **Schwab Trader API** - Account and position data
3. **Schwab Market Data API** - Option chain data

All clients are located in `wheel/clients/` directory.











## Financial Modeling Prep (FMP) Client

### Implementation
- **File**: `wheel/clients/fmp_stable_client.py`
- **Base URL**: `https://financialmodelingprep.com/stable`
- **Authentication**: API key in query parameter (`apikey`)
- **Version**: `fmp_stable_v1` (constant in file)

### Endpoints Used

#### Company Screener
- **Method**: `company_screener(exchange, sector, industry, limit)`
- **Endpoint**: `/stable/company-screener`
- **Purpose**: Build universe of stocks
- **Rate Limit**: 300 calls/minute (Starter plan)
- **Quirks**: 
  - Returns list of companies
  - Field names may vary (handles both `symbol` and `Symbol`)
  - Limit per exchange: 500 (to avoid timeouts)

#### Profile
- **Method**: `profile(symbol)` or `profile_many(symbols, chunk_size=50)`
- **Endpoint**: `/stable/profile?symbol=...`
- **Purpose**: Company profile (name, sector, industry, beta, market cap)
- **Batch Support**: Can fetch multiple symbols (comma-separated)
- **Fallback**: If batch fails, falls back to individual requests
- **Returns**: Dictionary or `{}` if not found

#### Quote
- **Method**: `quote(symbol)` or `quote_many(symbols, chunk_size=50)`
- **Endpoint**: `/stable/quote?symbol=...`
- **Purpose**: Current price, 52-week high/low
- **Batch Support**: Same as profile
- **Returns**: Dictionary or `{}` if not found

#### Ratios TTM
- **Method**: `ratios_ttm(symbol)`
- **Endpoint**: `/stable/ratios-ttm?symbol=...`
- **Purpose**: Financial ratios (profit margins, ROE, P/E, debt/equity)
- **Returns**: Dictionary or `{}` if not found

#### Key Metrics TTM
- **Method**: `key_metrics_ttm(symbol)`
- **Endpoint**: `/stable/key-metrics-ttm?symbol=...`
- **Purpose**: Additional financial metrics
- **Returns**: Dictionary or `{}` if not found

#### Technical Indicators - RSI
- **Method**: `technical_indicator_rsi(symbol, period=14, timeframe='1day')`
- **Endpoint**: `/stable/technical-indicators/rsi?symbol=...&periodLength=14&timeframe=1day`
- **Purpose**: RSI technical indicator (standard RSI(14), interval=1day)
- **Parameters**:
  - `periodLength`: RSI period (default: 14, configurable via `RSI_PERIOD`)
  - `timeframe`: "1day" (default, configurable via `RSI_INTERVAL`)
- **Returns**: Float (latest RSI value) or `None` if not available
- **Response Format**: Array of objects with `date`, `rsi`, `open`, `high`, `low`, `close`, `volume`
- **Quirks**: 
  - Returns array (not single value)
  - First item is most recent (sorted by date descending)
  - Must extract `rsi` field from first object
- **Note**: RSI is sourced exclusively from FMP (no Alpha Vantage dependency)

#### Stock News
- **Method**: `stock_news(symbol, limit=50)`
- **Endpoint**: `/stable/stock_news?tickers=...&limit=50`
- **Purpose**: Recent news articles for sentiment analysis
- **Returns**: List of news items or `[]` if not found

### Error Handling

#### Non-Fatal Errors
- **404 Not Found**: Returns `None` or `{}` (doesn't crash)
- **402 Payment Required**: Returns `None` (premium endpoint, logged as warning)
- **Other HTTP Errors**: Logs warning, returns empty/default value

#### Retry Logic
- Uses `tenacity` library for retries
- **Max Attempts**: 3
- **Backoff**: Exponential (1s to 15s)
- **Retries On**: `requests.HTTPError` (except 404)
- **No Retry On**: 404 (resource doesn't exist)

#### API Key Redaction
- `_redact_apikey()` helper function
- Redacts API key from error logs (security)
- Pattern: `apikey=REDACTED` in URLs

### Sentiment Scoring
- **Function**: `simple_sentiment_score(news_items)` (module-level function)
- **Location**: `wheel/clients/fmp_stable_client.py`
- **Algorithm**: Keyword-based (positive/negative word matching)
- **Returns**: Float in range [-1, 1]
- **Keywords**:
  - Positive: "beat", "surge", "upgrade", "growth", etc.
  - Negative: "miss", "plunge", "downgrade", "lawsuit", etc.

### Known Issues

#### Field Name Variations
- FMP API sometimes returns different field names (e.g., `symbol` vs `Symbol`)
- Code handles both variations with fallback logic
- Example: `company.get("symbol") or company.get("Symbol")`

#### Batch Request Failures
- `profile_many()` and `quote_many()` have fallback to individual requests
- If batch fails, processes symbols one-by-one
- Logs warning but continues processing

#### Premium Endpoints
- Some endpoints may return 402 (Payment Required) for certain symbols
- RSI endpoint may require premium for some tickers
- Code handles gracefully (returns `None`, logs warning)











## Schwab Trader API Client

### Implementation
- **File**: `wheel/clients/schwab_client.py`
- **Base URL**: `https://api.schwabapi.com/trader/v1`
- **Authentication**: OAuth 2.0 (see `authFlow.md`)
- **Purpose**: Account data, positions, orders, transactions

### Key Methods

#### Account Operations
- **`get_accounts()`**: List all accounts
- **`get_account(fields='positions')`**: Get single account with optional fields
- **`get_account_numbers()`**: Get account number to hashValue mappings
- **`_resolve_account_hash()`**: Resolves account hashValue automatically (cached)

#### Position Operations
- **`get_positions()`**: Convenience method to get positions from account

#### Order Operations
- **`get_orders()`**: Get order history (read-only)

#### Transaction Operations
- **`get_transactions()`**: Get transaction history (read-only)

### Account Hash Resolution

**Problem**: Schwab Trader API requires `hashValue` (not `accountNumber`) for some endpoints.

**Solution**: `_resolve_account_hash()` method:
1. Calls `/accounts/accountNumbers` endpoint
2. Extracts `hashValue` from response
3. Caches in `self._account_hash`
4. Handles single-account scenarios automatically

**Usage**: Caller doesn't need to know about hash - `get_account()` resolves it internally.

### Error Handling

#### Token Refresh
- Auto-refreshes access token on 401 (Unauthorized)
- Retries request after refresh
- Caches token in memory with expiry tracking

#### Rate Limiting
- Handles 429 (rate limit) with retry logic
- Uses exponential backoff
- Logs warnings but continues

#### Network Errors
- Timeout: 20 seconds
- Retries on transient failures
- Logs errors with context

### Known Issues

#### Account Hash Required
- Some endpoints require `hashValue` not present in `/accounts` response
- Workaround: Use `/accounts/accountNumbers` endpoint to get hashValue
- Implementation: Automatic resolution in `_resolve_account_hash()`

#### Multi-Account Scenarios
- Currently assumes single account
- If multiple accounts, uses first one with warning
- `SCHWAB_ACCOUNT_ID` env var not currently used (future enhancement)











## Schwab Market Data API Client

### Implementation
- **File**: `wheel/clients/schwab_marketdata_client.py`
- **Base URL**: `https://api.schwabapi.com`
- **Authentication**: OAuth 2.0 (separate from Trader API)
- **Purpose**: Option chain data

### Key Methods

#### Option Chain
- **`get_option_chain(symbol, contract_type='PUT', strike_count=50)`**
- **Endpoint**: `/marketdata/v1/chains`
- **Parameters**:
  - `symbol`: Stock symbol
  - `contractType`: "PUT" or "CALL"
  - `strikeCount`: Number of strikes (default: 50)
- **Returns**: Option chain JSON (structure varies)

### Authentication

#### Options
1. **Direct Access Token**: If `SCHWAB_ACCESS_TOKEN` env var is set, uses it directly
2. **Refresh Token Flow**: If `SCHWAB_REFRESH_TOKEN` is set, refreshes automatically

#### Token Refresh
- Uses Basic auth with `client_id:client_secret` for token endpoint
- Caches token in memory with expiry tracking
- Auto-refreshes when needed

### Error Handling

#### Auth Errors
- Raises `SchwabAuthError` exception
- Logs error with details (but redacts tokens)
- Fails fast (doesn't retry auth failures)

#### API Errors
- Logs error with status code and response body (truncated)
- Raises HTTPError for non-2xx responses
- Timeout: 30 seconds

### Known Issues

#### Option Chain Structure
- Response structure may vary
- Code does best-effort parsing
- May need adjustment if Schwab changes response format

#### Separate Auth Flow
- Market Data API uses different auth than Trader API
- Requires separate access token or refresh token
- Can use same refresh token as Trader API (if available)











## Supabase Client

### Implementation
- **File**: `wheel/clients/supabase_client.py`
- **Library**: `supabase-py` (v2.6.0)
- **Purpose**: Database operations (PostgreSQL via Supabase)

### Key Methods

#### Connection
- **`get_supabase()`**: Creates Supabase client (singleton pattern)
- **Environment Variables**: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`
- **Backwards Compatible**: `get_supabase_client()` alias

#### Insert Operations
- **`insert_row(table, row)`**: Insert single row
- **Returns**: Inserted row (dict) or `{}` if empty
- **Error Handling**: Raises `RuntimeError` on Supabase errors

#### Upsert Operations
- **`upsert_rows(table, rows, key=None, keys=None)`**: Upsert multiple rows
- **Deduplication**: Deduplicates in-Python before sending to Supabase
- **Composite Keys**: Supports `keys=['run_id', 'ticker']` for composite keys
- **Default Keys**: Auto-detects keys by table name:
  - `tickers`: `key='ticker'`
  - `screening_candidates`, `screening_picks`: `keys=['run_id', 'ticker']`
  - Others: `key='id'`
- **Why Deduplication**: Prevents Postgres error 21000 (duplicate key in same batch)

#### Update Operations
- **`update_rows(table, match, values)`**: Update rows matching criteria
- **Example**: `update_rows("screening_runs", {"run_id": run_id}, {"status": "success"})`

#### Select Operations
- **`select_all(table_or_view, limit=100)`**: Select all rows from table or view
- **Purpose**: Used by dashboard for reading views
- **Returns**: List of dictionaries
- **Error Handling**: Raises `RuntimeError` on query failures

### Error Handling

#### Supabase Errors
- `_raise_if_error()` helper checks for `.error` attribute
- Raises `RuntimeError` with context message
- Logs full error for debugging

#### Composite Key Deduplication
- Prevents Postgres error 21000 (duplicate key in same batch)
- Deduplicates in-Python before sending to Supabase
- Last row wins (if duplicates in batch)

### Known Issues

#### Type Hints
- Uses `Optional[str]` instead of `str | None` (Python 3.9 compatibility)
- Render uses Python 3.12, but code is compatible with 3.9+

#### Upsert Return Value
- `upsert_rows()` returns `res.data` (list of upserted rows)
- May be empty list if no rows upserted
- Caller should check return value if needed











## Common Patterns

### Retry Logic
All API clients use `tenacity` for retries:
- **Max Attempts**: 3
- **Backoff**: Exponential (1s to 15s or 2s to 20s)
- **Retries On**: `requests.HTTPError` (except 404)
- **No Retry On**: 404 (resource doesn't exist)

### Error Logging
- All clients log errors with context
- API keys/tokens redacted from logs
- Warnings for non-fatal errors (don't crash)
- Exceptions for fatal errors (fail fast)

### Timeout Handling
- FMP: 30 seconds
- Schwab Trader: 20 seconds
- Schwab Market Data: 30 seconds
- Supabase: Default (library handles)

### Non-Fatal Errors
- 404 Not Found: Returns `None` or `{}` (doesn't crash)
- 402 Payment Required: Returns `None` (premium endpoint)
- Missing data: Returns default values, continues processing











## Related Files

- `wheel/clients/fmp_stable_client.py` - FMP API client
- `wheel/clients/schwab_client.py` - Schwab Trader API client
- `wheel/clients/schwab_marketdata_client.py` - Schwab Market Data API client
- `wheel/clients/supabase_client.py` - Database client
- `Documentation/authFlow.md` - Authentication details


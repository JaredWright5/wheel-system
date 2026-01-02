# Known Issues & Technical Debt

This document catalogs known issues, workarounds, quirks, and areas for improvement in the codebase.

**Target Audience**: Developers, LLM assistants, project managers

**Purpose**: Save investigation time by documenting problems, solutions, and technical debt upfront.











## Critical Issues

### None Currently
No critical issues that prevent the system from functioning.











## Known Issues

### 1. Duplicate Alpha Vantage Client Files

**Issue**: Two similar files exist:
- `wheel/clients/alpha_vantage_client.py` (current, with throttling)
- `wheel/clients/alphavantage_client.py` (old, unused)

**Status**: Old file should be removed (no longer used)

**Impact**: Low (doesn't affect functionality, just clutter)

**Fix**: Delete `wheel/clients/alphavantage_client.py`

**Location**: `wheel/clients/alphavantage_client.py`

### 2. Render Cron Schedule Doesn't Adjust for DST

**Issue**: Cron schedules are hardcoded to UTC and don't adjust for Daylight Saving Time.

**Current Schedule**: `"30 12 * * 1"` = 4:30 AM PT during PST (standard time)

**Problem**: During PDT (daylight time), this becomes 5:30 AM PT (not 4:30 AM PT)

**Impact**: Medium (jobs run 1 hour later during daylight time)

**Workaround**: Manually update `render.yaml` schedule:
- PST: `"30 12 * * 1"` (12:30 UTC)
- PDT: `"30 11 * * 1"` (11:30 UTC)

**Future Fix**: Could use timezone-aware scheduling or adjust schedule twice per year

**Location**: `render.yaml`

### 3. CSP Picks May Run Before Screener Completes

**Issue**: `wheel-build-csp-picks` runs at same time as `wheel-weekly-screener` (4:30 AM PT Monday).

**Problem**: If screener takes longer than expected, CSP picks may use previous run's data.

**Impact**: Low (CSP picks script loads latest `run_id`, so it will use previous run if current not complete)

**Workaround**: CSP picks script is idempotent (can be re-run)

**Future Fix**: Stagger schedules (screener at 4:30 AM, picks at 4:45 AM)

**Location**: `render.yaml`

### 4. Earnings Calendar Logic Disabled

**Issue**: Earnings filtering is disabled in `weekly_screener.py` due to FMP legacy endpoint issues.

**Code Location**: `apps/worker/src/weekly_screener.py` line ~526

**Comment**: `"earn_in_days": None,  # TODO: Add earnings calendar logic"`

**Impact**: Medium (can't filter out stocks with earnings coming up)

**Future Fix**: Re-implement using FMP stable earnings calendar endpoint or alternative source

**Location**: `apps/worker/src/weekly_screener.py`

### 5. IV/IV Rank Not Populated in Screening

**Issue**: `screening_candidates` table has `iv` and `iv_rank` columns but they're always `NULL`.

**Reason**: IV data comes from Schwab option chains (not available during screening).

**Impact**: Low (IV populated later in pick generation)

**Note**: Comment in code: `"IV sourced from Schwab in pick builder; weekly_screener does not require IV to run"`

**Location**: `apps/worker/src/weekly_screener.py`

### 6. Dashboard No Authentication

**Issue**: Dashboard is publicly accessible (no authentication).

**Impact**: Low (URL not publicized, but security risk if exposed)

**Future Fix**: Add basic auth or OAuth

**Location**: `apps/dashboard/app.py`

### 7. Timezone Display in Dashboard

**Issue**: Timestamps displayed in UTC (not converted to local time).

**Impact**: Low (acceptable for now, but could be confusing)

**Future Fix**: Add timezone conversion in templates or JavaScript

**Location**: `apps/dashboard/templates/*.html`











## Workarounds & Quirks

### 1. Composite Key Deduplication

**Issue**: Postgres error 21000 (duplicate key in same batch) when upserting rows with composite keys.

**Workaround**: `upsert_rows()` deduplicates in-Python before sending to Supabase.

**Location**: `wheel/clients/supabase_client.py:upsert_rows()`

**Why**: Supabase/Postgres doesn't handle duplicate keys in same batch well.

**Impact**: None (handled automatically)

### 2. Account Hash Resolution

**Issue**: Schwab Trader API requires `hashValue` (not `accountNumber`) for some endpoints.

**Workaround**: `_resolve_account_hash()` method automatically resolves and caches hashValue.

**Location**: `wheel/clients/schwab_client.py:_resolve_account_hash()`

**Why**: Schwab API design (hashValue not in `/accounts` response).

**Impact**: None (handled automatically)

### 3. FMP Field Name Variations

**Issue**: FMP API sometimes returns different field names (e.g., `symbol` vs `Symbol`).

**Workaround**: Code handles both variations with fallback logic.

**Example**: `company.get("symbol") or company.get("Symbol")`

**Location**: Multiple files using FMP data

**Why**: FMP API inconsistency.

**Impact**: None (handled with fallbacks)

### 4. RSI Caching Strategy

**Issue**: FMP API has rate limits (300 calls/minute), but screening needs RSI for all tickers.

**Workaround**: RSI fetched once per day and cached in `rsi_snapshots` table.

**Location**: `apps/worker/src/rsi_snapshot.py`, `apps/worker/src/weekly_screener.py`

**Why**: Avoids rate limits during screening.

**Trade-off**: RSI data is up to 24 hours old (acceptable for weekly screening).

**Impact**: None (by design)

### 5. Type Hints for Python 3.9 Compatibility

**Issue**: Code uses `Optional[str]` instead of `str | None` (Python 3.12 syntax).

**Reason**: Render uses Python 3.12, but code is compatible with 3.9+.

**Location**: `wheel/clients/supabase_client.py` and others

**Impact**: None (just style preference)

### 6. View Error Detection Edge Case

**Issue**: `_safe_select()` may return `has_error=True` even when view exists but is empty.

**Workaround**: Templates check both `has_error` and `len(data) == 0`.

**Location**: `apps/dashboard/app.py:_safe_select()`, templates

**Impact**: None (handled in templates)

### 7. Batch Request Fallback

**Issue**: FMP batch requests (`profile_many()`, `quote_many()`) may fail for some symbols.

**Workaround**: Falls back to individual requests if batch fails.

**Location**: `wheel/clients/fmp_stable_client.py:profile_many()`, `quote_many()`

**Why**: FMP API may reject some symbols in batch.

**Impact**: None (handled automatically, just slower)











## Technical Debt

### 1. Unused Package Structure

**Issue**: `packages/core/src/` directory exists but is mostly empty (legacy structure).

**Status**: Can be removed (code moved to `wheel/` package)

**Location**: `packages/` directory

**Impact**: Low (just clutter)

### 2. Unused Email Alerts

**Issue**: `wheel/alerts/emailer.py` exists but is not used (dashboard-first approach).

**Status**: Can be removed or kept for future use

**Location**: `wheel/alerts/emailer.py`

**Impact**: None (just unused code)

### 3. Temporary Test Files

**Issue**: Several `tmp_*.py` files in root directory (test scripts).

**Files**: 
- `tmp_check_hash.py`
- `tmp_find_hash.py`
- `tmp_show_accounts.py`
- `tmp_show_hash.py`
- `tmp_test_accounts_structure.py`
- `test_schwab_accounts.py`

**Status**: Should be moved to `tests/` or removed

**Impact**: Low (just clutter)

### 4. Legacy SQL Files

**Issue**: `sql/` directory has placeholder files (`001_init.sql` is empty).

**Status**: Migrations are in `supabase/migrations/`, `sql/` can be removed

**Location**: `sql/` directory

**Impact**: Low (just clutter)

### 5. Build Picks Script Not in Render

**Issue**: `build_cc_picks.py` exists but cron job not in `render.yaml`.

**Status**: Needs to be added to `render.yaml`

**Location**: `render.yaml` (missing), `apps/worker/src/build_cc_picks.py` (exists)

**Impact**: Medium (CC picks not generated automatically)

### 6. No Unit Tests

**Issue**: No unit tests for core logic (scoring, filtering, etc.).

**Status**: All testing is via smoke tests and manual runs

**Impact**: Medium (harder to refactor safely)

**Future**: Add pytest tests for core functions

### 7. Limited Error Recovery

**Issue**: If weekly screener fails partway through, partial data may be written.

**Status**: Run status tracking helps, but no automatic retry

**Impact**: Low (can manually re-run)

**Future**: Add retry logic or idempotent re-runs











## Code Quality Issues

### 1. Inconsistent Error Handling

**Issue**: Some functions return `None` on error, others raise exceptions.

**Examples**:
- FMP client: Returns `{}` or `None` on errors (non-fatal)
- Supabase client: Raises `RuntimeError` on errors (fatal)

**Impact**: Low (by design, but could be more consistent)

### 2. Magic Numbers

**Issue**: Some hardcoded values (e.g., batch size 50, limit 25).

**Examples**:
- `upsert_rows()` batch size: 50
- `PICKS_N` default: 25
- `MAX_REQUESTS_PER_DAY`: 24 (Alpha Vantage, now unused)

**Impact**: Low (but could be configurable)

### 3. Long Functions

**Issue**: Some functions are quite long (e.g., `weekly_screener.py:main()` is ~300 lines).

**Impact**: Low (but harder to test and maintain)

**Future**: Break into smaller functions

### 4. Duplicate Code

**Issue**: Some logic duplicated across files (e.g., universe loading, expiration selection).

**Examples**:
- Universe loading: `weekly_screener.py` and `rsi_snapshot.py`
- Expiration selection: `build_csp_picks.py` and `build_cc_picks.py`

**Impact**: Low (but could be extracted to shared functions)

**Future**: Extract to shared utility functions











## API-Specific Issues

### 1. FMP Premium Endpoints

**Issue**: Some FMP endpoints return 402 (Payment Required) for certain symbols.

**Examples**: RSI for some tickers, news for some tickers

**Workaround**: Code handles gracefully (returns `None`, logs warning)

**Impact**: Low (missing data, but doesn't crash)

**Location**: `wheel/clients/fmp_stable_client.py`

### 2. Schwab Option Chain Structure

**Issue**: Schwab option chain response structure may vary.

**Workaround**: Code does best-effort parsing

**Impact**: Low (may need adjustment if Schwab changes format)

**Location**: `apps/worker/src/build_csp_picks.py`, `build_cc_picks.py`

### 3. Schwab Multi-Account Support

**Issue**: Code assumes single account (uses first account if multiple).

**Workaround**: `SCHWAB_ACCOUNT_ID` env var exists but not used

**Impact**: Low (works for single-account scenarios)

**Future**: Add proper multi-account support

**Location**: `wheel/clients/schwab_client.py`











## Database Issues

### 1. No Data Retention Policy

**Issue**: No automatic cleanup of old data (runs, snapshots accumulate).

**Impact**: Medium (database will grow over time)

**Future**: Add data retention policy or archival

### 2. No Index on Some Queries

**Issue**: Some queries may not use indexes efficiently.

**Examples**: Queries by `ticker` without `run_id` may be slow

**Impact**: Low (acceptable for current data volume)

**Future**: Add composite indexes if needed

### 3. JSONB Fields Not Indexed

**Issue**: `metrics` and `pick_metrics` JSONB fields are not indexed.

**Impact**: Low (not queried directly, just stored)

**Future**: Add GIN indexes if JSONB queries needed











## Documentation Issues

### 1. Incomplete README

**Issue**: Main README exists but could be more comprehensive.

**Status**: ✅ Fixed (updated in this session)

### 2. No API Documentation

**Issue**: No OpenAPI/Swagger docs for dashboard API.

**Impact**: Low (dashboard is simple, but could be helpful)

**Future**: Add FastAPI auto-generated docs (`/docs` endpoint)











## Performance Considerations

### 1. Sequential Processing

**Issue**: All tickers processed sequentially (not parallelized).

**Impact**: Low (acceptable for ~168 tickers, but could be faster)

**Future**: Add parallel processing with `concurrent.futures`

### 2. No Caching of FMP Data

**Issue**: FMP data (profile, quote, etc.) fetched every run (not cached).

**Impact**: Low (acceptable, but could cache for faster re-runs)

**Future**: Add caching layer (Redis or database)

### 3. Large JSONB Fields

**Issue**: `metrics` and `pick_metrics` fields store full API responses (can be large).

**Impact**: Low (helpful for debugging, but increases storage)

**Future**: Consider archiving old metrics or compressing











## Security Considerations

### 1. API Keys in Environment Variables

**Issue**: API keys stored in environment variables (not encrypted at rest in Render).

**Status**: Acceptable (Render encrypts at rest, but could be better)

**Future**: Consider using secrets management service

### 2. No Input Validation

**Issue**: Dashboard doesn't validate user input (read-only, but still good practice).

**Impact**: Low (read-only, but could add validation)

**Future**: Add Pydantic models for request validation

### 3. No Rate Limiting

**Issue**: Dashboard has no rate limiting (could be abused).

**Impact**: Low (internal use, but could add rate limiting)

**Future**: Add rate limiting middleware











## Recommendations

### High Priority
1. Remove duplicate/unused files (`alphavantage_client.py`, `tmp_*.py`)
2. Add `wheel-build-cc-picks` to `render.yaml`
3. Stagger Monday cron schedules to avoid conflicts

### Medium Priority
1. Re-implement earnings calendar filtering
2. Add unit tests for core logic
3. Extract duplicate code to shared utilities
4. Add data retention policy

### Low Priority
1. Add authentication to dashboard
2. Add timezone conversion in dashboard
3. Add parallel processing for screening
4. Add FastAPI auto-generated docs











## Related Files

- `render.yaml` - Deployment configuration (schedule issues)
- `apps/worker/src/weekly_screener.py` - Main screening logic (earnings TODO)
- `wheel/clients/*.py` - API clients (various quirks)
- `apps/dashboard/app.py` - Dashboard (auth, timezone issues)
- `supabase/migrations/*.sql` - Database schema (indexes, retention)


# Authentication Flow

This document explains how the system authenticates with external APIs, particularly the Schwab API which uses OAuth 2.0.

**Target Audience**: Project managers, developers working on API integrations, LLM assistants

**Purpose**: Understand authentication mechanisms, token management, and any quirks or workarounds.











## Overview

The Wheel System integrates with two main external APIs that require authentication:
1. **Financial Modeling Prep (FMP)** - API key authentication (simple)
2. **Schwab API** - OAuth 2.0 with refresh tokens (complex)











## FMP Authentication

### Implementation
- **Location**: `wheel/clients/fmp_stable_client.py`
- **Method**: API key in query parameter
- **Environment Variable**: `FMP_API_KEY`
- **Usage**: Added automatically to all requests via `_get()` method

### Key Points
- API key is redacted from error logs (security best practice)
- No token refresh needed (static API key)
- Rate limits: 300 calls/minute on Starter plan











## Schwab API Authentication

### Overview
Schwab uses **OAuth 2.0 authorization code flow** with refresh tokens. The system uses two separate Schwab APIs:
1. **Trader API** - Account data, positions, orders (`schwab_client.py`)
2. **Market Data API** - Option chains (`schwab_marketdata_client.py`)

### OAuth 2.0 Flow

#### Initial Setup (One-Time)
1. Register application with Schwab Developer Portal
2. Obtain `CLIENT_ID` and `CLIENT_SECRET`
3. Set redirect URI (must match registered URI)
4. User authorizes application → receives authorization code
5. Exchange authorization code for refresh token (one-time)
6. Store refresh token securely (environment variable)

#### Runtime Flow (Automatic)
```
1. System starts with SCHWAB_REFRESH_TOKEN in environment
2. When API call needed:
   a. Check if access_token exists and is valid
   b. If expired/missing: Refresh using refresh_token
   c. Use access_token for API request
3. Access token cached in memory (with expiry buffer)
4. Auto-refreshes when needed (transparent to caller)
```

### Implementation Details

#### Trader API Client (`wheel/clients/schwab_client.py`)

**Key Methods**:
- `from_env()` - Factory method to create client from environment variables
- `refresh_access_token()` - Exchanges refresh token for new access token
- `access_token` (property) - Returns valid access token (auto-refreshes)
- `_request()` - Makes authenticated API calls with retry logic

**Environment Variables Required**:
- `SCHWAB_CLIENT_ID`
- `SCHWAB_CLIENT_SECRET`
- `SCHWAB_REFRESH_TOKEN`
- `SCHWAB_ACCOUNT_ID` (optional, for multi-account scenarios)

**Token Caching**:
- Access token stored in `self._access_token`
- Expiry tracked in `self._access_token_expiry_epoch`
- Refreshes automatically if within 30 seconds of expiry (buffer)

**Account Hash Resolution**:
- Schwab Trader API requires `hashValue` (not `accountNumber`) for some endpoints
- `_resolve_account_hash()` method:
  - Calls `/accounts/accountNumbers` endpoint
  - Extracts `hashValue` from response
  - Caches in `self._account_hash`
  - Handles single-account scenarios automatically

**Quirks & Workarounds**:
- Authorization codes expire quickly (must be used immediately)
- Account hash resolution is automatic (caller doesn't need to know about it)
- `get_account()` method resolves hash internally (no `account_id` parameter needed)

#### Market Data API Client (`wheel/clients/schwab_marketdata_client.py`)

**Key Methods**:
- `_get_bearer_token()` - Gets valid access token (direct or refresh)
- `_request()` - Makes authenticated API calls
- `get_option_chain()` - Fetches option chain data

**Authentication Options**:
1. **Direct Access Token**: If `SCHWAB_ACCESS_TOKEN` env var is set, uses it directly
2. **Refresh Token Flow**: If `SCHWAB_REFRESH_TOKEN` is set, refreshes automatically

**Token Refresh Logic**:
- Uses Basic auth with `client_id:client_secret` for token endpoint
- Caches token in memory with expiry tracking
- Auto-refreshes when needed

**Quirks & Workarounds**:
- Market Data API uses different auth flow than Trader API
- Can use either direct access token OR refresh token (flexible)
- Token endpoint uses Basic auth (not Bearer)











## Error Handling

### Token Refresh Failures
- If refresh fails, raises `SchwabAuthError` exception
- Logs error with details (but redacts sensitive data)
- Workers should handle gracefully (log and continue, or fail fast)

### Rate Limiting
- Schwab API may return 429 (rate limit)
- `_request()` method includes retry logic for 429
- Also retries on 401 (token refresh, then retry)

### Network Errors
- Uses `tenacity` for retry logic (exponential backoff)
- Timeout: 20-30 seconds depending on client
- Logs errors but doesn't crash entire run











## Security Considerations

### API Key Storage
- All API keys stored in environment variables (never in code)
- `.env.local` file is git-ignored
- Render environment variables set in dashboard (encrypted)

### Token Redaction
- FMP API key redacted from error logs
- Schwab tokens not logged (security best practice)
- Error messages sanitized before logging

### Refresh Token Security
- Refresh tokens are long-lived (don't expire)
- Must be kept secure (environment variables only)
- If compromised, revoke in Schwab Developer Portal











## Testing Authentication

### Smoke Tests
- `apps/worker/src/schwab_smoketest.py` - Tests Trader API connection
- Verifies token refresh works
- Tests account data retrieval

### Manual Testing
```bash
# Test Schwab connection
PYTHONPATH=. python -m apps.worker.src.schwab_smoketest

# Test FMP connection
PYTHONPATH=. python -m apps.worker.src.fmp_stable_smoketest
```











## Common Issues

### Issue: "Missing SCHWAB_REFRESH_TOKEN"
- **Cause**: Environment variable not set
- **Fix**: Add to `.env.local` or Render environment variables
- **Location**: `wheel/clients/schwab_client.py:from_env()`

### Issue: "Token refresh failed: 401"
- **Cause**: Refresh token expired or invalid
- **Fix**: Re-authorize application and get new refresh token
- **Note**: Refresh tokens shouldn't expire, but can be revoked

### Issue: "Invalid account number"
- **Cause**: Using `accountNumber` instead of `hashValue` for Trader API
- **Fix**: Use `_resolve_account_hash()` method (automatic in `get_account()`)
- **Location**: `wheel/clients/schwab_client.py:_resolve_account_hash()`

### Issue: "Authorization code expired"
- **Cause**: Authorization codes are short-lived (must be used immediately)
- **Fix**: Get new authorization code and exchange immediately
- **Note**: This only happens during initial setup











## Future Improvements

- Consider token encryption at rest (if storing in database)
- Add token rotation mechanism (if Schwab supports it)
- Implement token refresh retry with exponential backoff
- Add monitoring/alerting for auth failures











## Related Files

- `wheel/clients/schwab_client.py` - Trader API client
- `wheel/clients/schwab_marketdata_client.py` - Market Data API client
- `apps/worker/src/schwab_smoketest.py` - Authentication test script
- `.env.local.example` - Environment variable template


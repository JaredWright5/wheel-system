# Wheel System - Options Trading Automation Platform

A Python-based automated options trading system that screens stocks, generates trading picks (Cash-Secured Puts and Covered Calls), and tracks positions using market data APIs and broker integrations.

## Overview

The Wheel System automates the "wheel strategy" for options trading by:
1. **Screening** stocks weekly based on fundamentals, sentiment, and technical indicators
2. **Generating Picks** for Cash-Secured Puts (CSP) and Covered Calls (CC) with optimal strike/delta selection
3. **Tracking** account positions and performance daily
4. **Dashboard** for viewing screening results and trading opportunities

## Technology Stack

### Languages & Frameworks
- **Python 3.12.8** - Primary language (pinned in `runtime.txt`)
- **FastAPI** - Web framework for dashboard (`apps/dashboard/app.py`)
- **Jinja2** - Server-side templating for HTML rendering
- **Uvicorn** - ASGI server for FastAPI

### Core Libraries
- **requests** - HTTP client for API calls
- **supabase** - PostgreSQL database client (Supabase)
- **loguru** - Structured logging
- **tenacity** - Retry logic with exponential backoff
- **python-dotenv** - Environment variable management
- **pydantic** - Data validation (minimal usage)

### External Services & APIs
- **Financial Modeling Prep (FMP)** - Market data, fundamentals, technical indicators (RSI)
- **Schwab API** - Broker integration (account data, positions, option chains)
- **Supabase** - PostgreSQL database (hosted)
- **Render** - Cloud hosting and cron job scheduling

### Infrastructure
- **Render.com** - Hosting platform
  - Web service: Dashboard (`wheel-dashboard`)
  - Cron jobs: 6 scheduled workers
- **Supabase** - Database and migrations
- **GitHub** - Version control

## Project Structure

```
wheel-system/
├── apps/
│   ├── dashboard/          # FastAPI web application
│   │   ├── app.py         # Main FastAPI app
│   │   ├── templates/     # Jinja2 HTML templates
│   │   └── static/        # CSS files
│   └── worker/            # Background workers
│       └── src/
│           ├── weekly_screener.py    # Main screening logic
│           ├── rsi_snapshot.py      # RSI data caching
│           ├── build_csp_picks.py   # CSP pick generation
│           ├── build_cc_picks.py    # CC pick generation
│           ├── daily_tracker.py     # Account snapshots
│           └── *_smoketest.py       # Test scripts
├── wheel/                  # Core package (shared code)
│   ├── clients/            # API clients
│   │   ├── fmp_stable_client.py     # FMP API client
│   │   ├── schwab_client.py         # Schwab Trader API
│   │   ├── schwab_marketdata_client.py  # Schwab Market Data API
│   │   └── supabase_client.py      # Database client
│   └── alerts/
│       └── emailer.py      # SMTP email alerts (unused)
├── supabase/
│   └── migrations/        # Database schema migrations
├── data/
│   └── universe_us.csv    # Static universe of stocks
├── Documentation/         # Project documentation (this folder)
├── render.yaml            # Render deployment configuration
├── requirements.txt       # Python dependencies
└── runtime.txt           # Python version pin

```

## Key Features

### 1. Weekly Stock Screening
- Fetches universe from CSV or FMP company screener
- Enriches with fundamentals, sentiment, technical indicators
- Scores candidates using weighted algorithm
- Stores results in `screening_candidates` table

### 2. RSI Caching
- Daily snapshot of RSI values for all universe tickers
- Cached in `rsi_snapshots` table to avoid API rate limits
- Used by weekly screener for technical scoring

### 3. Pick Generation
- **CSP Picks**: Cash-Secured Put options from top candidates
- **CC Picks**: Covered Call options from existing positions
- Tiered expiration selection (primary + fallback windows)
- Delta-based strike selection
- Annualized yield calculation

### 4. Account Tracking
- Daily snapshots of Schwab account balances
- Position tracking with market values
- Historical data in `account_snapshots` and `position_snapshots`

### 5. Dashboard
- Web interface for viewing screening results
- Run history, candidates, and picks
- Read-only views from Supabase

## Environment Variables

Required environment variables (see `.env.local.example`):
- `SUPABASE_URL` / `SUPABASE_SERVICE_ROLE_KEY` - Database connection
- `FMP_API_KEY` - Financial Modeling Prep API key
- `SCHWAB_CLIENT_ID` / `SCHWAB_CLIENT_SECRET` / `SCHWAB_REFRESH_TOKEN` - Schwab OAuth
- `SCHWAB_ACCOUNT_ID` - Optional, for multi-account scenarios

Optional configuration:
- `UNIVERSE_SOURCE` - "csv" (default) or "fmp_stable"
- `RSI_PERIOD` / `RSI_INTERVAL` - RSI calculation parameters
- `MIN_PRICE` / `MIN_MARKET_CAP` - Universe filters
- `MIN_DTE` / `MAX_DTE` - Option expiration windows

## Quick Start

1. **Clone and setup**:
   ```bash
   git clone <repo>
   cd wheel-system
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure environment**:
   ```bash
   cp .env.local.example .env.local
   # Edit .env.local with your API keys
   ```

3. **Database setup**:
   ```bash
   supabase login
   supabase link --project-ref <YOUR_PROJECT_REF>
   make db-push  # Apply migrations
   ```

4. **Local testing**:
   ```bash
   PYTHONPATH=. python -m apps.worker.src.db_smoketest
   PYTHONPATH=. python -m apps.worker.src.weekly_screener
   ```

5. **Deploy to Render**:
   - Connect GitHub repo to Render
   - Render will auto-deploy from `render.yaml`
   - Add environment variables in Render dashboard

## Documentation

Comprehensive documentation is available in the `Documentation/` folder:

- **overview.md** - System architecture and high-level flows
- **authFlow.md** - Schwab OAuth authentication
- **dataFlow.md** - Screening and pick generation workflows
- **apiClients.md** - External API integrations
- **database.md** - Schema, tables, views, migrations
- **deployment.md** - Render configuration and cron schedules
- **dashboard.md** - Web application structure
- **issues.md** - Known issues, workarounds, and technical debt

## Development

### Running Locally
- Workers: `PYTHONPATH=. python -m apps.worker.src.<script_name>`
- Dashboard: `PYTHONPATH=. uvicorn apps.dashboard.app:app --reload`

### Database Migrations
- Create: `supabase/migrations/YYYYMMDDHHMMSS_description.sql`
- Apply: `make db-push` or `supabase db push`
- Test: `make db-smoke`

### Testing
- Smoke tests: `apps/worker/src/*_smoketest.py`
- Run with: `PYTHONPATH=. python -m apps.worker.src.<test_name>`

## Deployment

The system is deployed on Render.com with:
- **1 Web Service**: Dashboard (FastAPI)
- **6 Cron Jobs**: Scheduled workers (weekly screener, daily tracker, pick builders, etc.)

See `render.yaml` for full configuration and schedules.

## License

Private project - All rights reserved.


# Deployment Documentation

This document describes the deployment configuration, cron schedules, and infrastructure setup.

**Target Audience**: DevOps, project managers, LLM assistants

**Purpose**: Understand how the system is deployed, when jobs run, and how to manage deployments.











## Overview

The Wheel System is deployed on **Render.com** using:
- **1 Web Service**: Dashboard (FastAPI)
- **6 Cron Jobs**: Scheduled background workers

Configuration is defined in `render.yaml` at the repository root.











## Render Configuration

### File: `render.yaml`

**Structure**:
- Services defined as YAML list
- Each service has: `type`, `name`, `runtime`, `buildCommand`, `startCommand`, `schedule` (cron only), `envVars`

**Build Process**:
- `buildCommand`: `pip install --upgrade pip && pip install -r requirements.txt`
- Installs Python dependencies from `requirements.txt`
- No custom build steps needed

**Python Version**:
- Pinned in `runtime.txt`: `python-3.12.8`
- Render uses this version for all services

**Python Path**:
- All services use: `PYTHONPATH=/opt/render/project/src`
- Allows imports like `from wheel.clients...` to work
- Render project root is `/opt/render/project/`











## Web Service

### wheel-dashboard
- **Type**: `web`
- **Runtime**: `python`
- **Start Command**: `PYTHONPATH=/opt/render/project/src uvicorn apps.dashboard.app:app --host 0.0.0.0 --port $PORT`
- **Port**: Uses `$PORT` environment variable (Render-provided)
- **Purpose**: FastAPI web application for viewing screening results
- **URL**: Provided by Render (e.g., `wheel-dashboard.onrender.com`)

**Environment Variables**:
- `PYTHONUNBUFFERED=1` - Ensures logs appear immediately
- `SUPABASE_URL` - Database connection (set in Render dashboard)
- `SUPABASE_SERVICE_ROLE_KEY` - Database auth (set in Render dashboard)











## Cron Jobs

All cron jobs run on **Python runtime** with the same build process.

### Schedule Format
- **Cron Syntax**: `"MM HH * * DOW"` (minute, hour, day of month, month, day of week)
- **Timezone**: UTC (Render cron is always UTC)
- **PT Conversion**: 12:30 UTC = 4:30 AM PT during PST (standard time)
- **DST Note**: Schedule doesn't auto-adjust for DST (manual update needed)

### 1. wheel-weekly-screener
- **Schedule**: `"30 12 * * 1"` - Monday 4:30 AM PT (12:30 UTC during PST)
- **Start Command**: `PYTHONPATH=/opt/render/project/src python -m apps.worker.src.weekly_screener`
- **Purpose**: Main stock screening process
- **Dependencies**: FMP API, Supabase, RSI cache
- **Duration**: ~10-30 minutes (depending on universe size)

### 2. wheel-daily-tracker
- **Schedule**: `"30 12 * * 2-6"` - Weekdays 4:30 AM PT (12:30 UTC during PST)
- **Start Command**: `PYTHONPATH=/opt/render/project/src python -m apps.worker.src.daily_tracker`
- **Purpose**: Account and position snapshots
- **Dependencies**: Schwab API, Supabase
- **Duration**: ~1-2 minutes

### 3. wheel-schwab-smoketest
- **Schedule**: `"25 12 * * *"` - Daily 4:25 AM PT (12:25 UTC during PST)
- **Start Command**: `PYTHONPATH=/opt/render/project/src python -m apps.worker.src.schwab_smoketest`
- **Purpose**: Verify Schwab API connectivity
- **Dependencies**: Schwab API
- **Duration**: ~10-30 seconds

### 4. wheel-build-csp-picks
- **Schedule**: `"30 12 * * 1"` - Monday 4:30 AM PT (12:30 UTC during PST)
- **Start Command**: `PYTHONPATH=/opt/render/project/src python -m apps.worker.src.build_csp_picks`
- **Purpose**: Generate Cash-Secured Put picks
- **Dependencies**: Supabase (candidates), Schwab Market Data API
- **Duration**: ~5-15 minutes (depends on number of candidates)
- **Note**: Runs same time as weekly screener (should run after screener completes)

### 5. wheel-rsi-snapshot
- **Schedule**: `"30 12 * * 1-5"` - Mon-Fri 4:30 AM PT (12:30 UTC during PST)
- **Start Command**: `PYTHONPATH=/opt/render/project/src python -m apps.worker.src.rsi_snapshot`
- **Purpose**: Cache RSI values for all universe tickers
- **Dependencies**: FMP API, Supabase
- **Duration**: ~5-10 minutes (168 tickers, FMP rate limit: 300/min)
- **Note**: Runs before weekly screener to populate cache

### 6. wheel-build-cc-picks
- **Schedule**: Not yet in `render.yaml` (needs to be added)
- **Start Command**: `PYTHONPATH=/opt/render/project/src python -m apps.worker.src.build_cc_picks`
- **Purpose**: Generate Covered Call picks
- **Dependencies**: Schwab Trader API (positions), Schwab Market Data API
- **Duration**: ~2-5 minutes (depends on number of positions)











## Environment Variables

### Required for All Services
- `PYTHONUNBUFFERED=1` - Set automatically in `render.yaml`

### Dashboard-Specific
- `SUPABASE_URL` - Supabase database URL
- `SUPABASE_SERVICE_ROLE_KEY` - Supabase service role key (for read access)

### Worker-Specific
- `SUPABASE_URL` - Supabase database URL
- `SUPABASE_SERVICE_ROLE_KEY` - Supabase service role key
- `FMP_API_KEY` - Financial Modeling Prep API key
- `SCHWAB_CLIENT_ID` - Schwab OAuth client ID
- `SCHWAB_CLIENT_SECRET` - Schwab OAuth client secret
- `SCHWAB_REFRESH_TOKEN` - Schwab OAuth refresh token
- `SCHWAB_ACCOUNT_ID` - Optional, for multi-account scenarios

### Optional Configuration
- `UNIVERSE_SOURCE` - "csv" (default) or "fmp_stable"
- `RSI_PERIOD` - RSI period (default: 14)
- `RSI_INTERVAL` - RSI interval (default: "daily")
- `RSI_MAX_AGE_HOURS` - Max age for cached RSI (default: 24)
- `MIN_PRICE` - Minimum stock price filter (default: 5.0)
- `MIN_MARKET_CAP` - Minimum market cap filter (default: 2000000000)
- `MIN_DTE`, `MAX_DTE` - Option expiration windows
- `PICKS_N` - Number of CSP picks to generate (default: 25)
- `CC_PICKS_N` - Number of CC picks to generate (default: 25)
- `CC_TEST_TICKERS` - Test mode for CC picks (comma-separated tickers)











## Deployment Process

### Initial Setup
1. **Connect GitHub** to Render
2. **Create Blueprint** from `render.yaml` (or create services manually)
3. **Add Environment Variables** in Render dashboard for each service
4. **Deploy** - Render auto-deploys on git push

### Ongoing Deployments
- **Automatic**: Render auto-deploys on git push to `main` branch
- **Manual**: Can trigger manual deploy in Render dashboard
- **Build Logs**: Available in Render dashboard for each service

### Environment Variable Management
- Set in Render dashboard → Service → Environment
- **Per-Service**: Each service has its own environment variables
- **Secrets**: Render encrypts environment variables at rest
- **Updates**: Changes require service restart (automatic)











## Cron Schedule Details

### Timezone Handling
- **Render Cron**: Always UTC (no timezone configuration)
- **PT Conversion**: 
  - PST (Standard Time): Subtract 8 hours (12:30 UTC = 4:30 AM PT)
  - PDT (Daylight Time): Subtract 7 hours (11:30 UTC = 4:30 AM PT)
- **Current Schedule**: `"30 12 * * 1"` = 4:30 AM PT during PST
- **DST Issue**: Schedule doesn't auto-adjust (would need manual update to `"30 11 * * 1"` during PDT)

### Schedule Conflicts
- **Monday 4:30 AM PT**: 
  - `wheel-weekly-screener` (runs first)
  - `wheel-build-csp-picks` (should run after screener, but same time)
  - `wheel-rsi-snapshot` (runs before screener to populate cache)
- **Potential Issue**: CSP picks may run before screener completes
- **Workaround**: CSP picks script loads latest `run_id`, so it will use previous run if current run not complete

### Recommended Schedule Adjustments
Consider staggering Monday jobs:
- `wheel-rsi-snapshot`: `"30 12 * * 1-5"` (Mon-Fri 4:30 AM PT) ✅
- `wheel-weekly-screener`: `"30 12 * * 1"` (Mon 4:30 AM PT) ✅
- `wheel-build-csp-picks`: `"45 12 * * 1"` (Mon 4:45 AM PT) - Give screener 15 min
- `wheel-build-cc-picks`: `"45 12 * * 1"` (Mon 4:45 AM PT) - Can run parallel with CSP











## Monitoring

### Render Dashboard
- **Logs**: Available in Render dashboard for each service
- **Status**: Shows service health and last run time
- **Alerts**: Can set up email alerts for service failures

### Application Logging
- **Library**: `loguru` (structured logging)
- **Format**: Timestamp, level, message
- **Levels**: INFO, WARNING, ERROR, EXCEPTION
- **Output**: Render captures stdout/stderr as logs

### Health Checks
- **Dashboard**: `/health` endpoint returns `{"ok": true}`
- **Workers**: No health check endpoint (cron jobs)

### Run Status Tracking
- **Database**: `screening_runs.status` field tracks run state
- **Values**: 'running', 'success', 'failed'
- **Error Field**: Stores error message if failed
- **Query**: Can query `screening_runs` table to see run history











## Troubleshooting

### Common Issues

#### Service Fails to Start
- **Check**: Environment variables are set
- **Check**: `requirements.txt` dependencies are valid
- **Check**: Python version matches `runtime.txt`
- **Check**: `PYTHONPATH` is set correctly

#### Cron Job Not Running
- **Check**: Schedule syntax is correct (cron format)
- **Check**: Service is enabled in Render dashboard
- **Check**: Render cron logs for errors
- **Check**: Environment variables are set for cron service

#### Import Errors
- **Check**: `PYTHONPATH=/opt/render/project/src` is set
- **Check**: `wheel/` package structure is correct
- **Check**: `__init__.py` files exist in package directories

#### Database Connection Errors
- **Check**: `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are set
- **Check**: Supabase project is active
- **Check**: Service role key has correct permissions

#### API Authentication Errors
- **Check**: API keys are set in environment variables
- **Check**: API keys are valid (not expired)
- **Check**: Schwab refresh token is valid (re-authorize if needed)











## Rollback Strategy

### Code Rollback
- **Git**: Revert commit and push
- **Render**: Auto-deploys on push (rolls back automatically)
- **Manual**: Can deploy specific commit in Render dashboard

### Database Rollback
- **Migrations**: Supabase tracks applied migrations
- **Manual**: Can revert migration by creating new migration
- **Data**: No automatic data rollback (manual restore if needed)











## Scaling Considerations

### Current Setup
- **Single Instance**: Each service runs on single instance
- **No Load Balancing**: Dashboard has no load balancer
- **Cron Jobs**: Run once per schedule (no parallelization)

### Future Scaling
- **Dashboard**: Can scale horizontally (add more instances)
- **Workers**: Cron jobs are single-instance (by design)
- **Database**: Supabase handles scaling (managed service)











## Related Files

- `render.yaml` - Deployment configuration
- `requirements.txt` - Python dependencies
- `runtime.txt` - Python version
- `apps/dashboard/app.py` - Dashboard application
- `apps/worker/src/*.py` - Worker scripts
- `Documentation/issues.md` - Known deployment issues


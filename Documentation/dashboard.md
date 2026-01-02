# Dashboard Documentation

This document describes the web dashboard application, its structure, routes, and functionality.

**Target Audience**: Frontend developers, project managers, LLM assistants

**Purpose**: Understand the dashboard architecture, how it displays data, and how to extend it.











## Overview

The dashboard is a **FastAPI web application** that provides a read-only interface for viewing screening results, candidates, and trading picks. The dashboard displays picks for manual review—users submit trades manually (the system does not place orders automatically).

**Technology Stack**:
- **FastAPI** - Web framework
- **Jinja2** - Server-side templating
- **Uvicorn** - ASGI server
- **Supabase** - Data source (read-only)

**Deployment**: Render.com web service (`wheel-dashboard`)











## Application Structure

### File: `apps/dashboard/app.py`

**Main Application**:
- FastAPI app instance: `app = FastAPI(title="Wheel System Dashboard v1")`
- Static files mounted at `/static`
- Templates directory: `apps/dashboard/templates/`
- Environment variable loading (conditional for local dev)

### Routes

#### `/health`
- **Method**: GET
- **Response**: `{"ok": true}`
- **Purpose**: Health check endpoint
- **Usage**: Render health checks, monitoring

#### `/` (Home)
- **Method**: GET
- **Response**: HTML (Jinja2 template)
- **Purpose**: Dashboard summary page
- **Data**: 
  - Latest 25 candidates
  - Latest CSP picks
  - Latest CC picks
  - Latest 5 runs (for summary)
- **Template**: `templates/index.html`

#### `/runs`
- **Method**: GET
- **Response**: HTML (Jinja2 template)
- **Purpose**: Run history page
- **Data**: Last 200 runs from `v_run_history` view
- **Template**: `templates/runs.html`

#### `/candidates`
- **Method**: GET
- **Response**: HTML (Jinja2 template)
- **Purpose**: Latest top 25 candidates page
- **Data**: Top 25 candidates from `v_latest_run_top25_candidates` view
- **Template**: `templates/candidates.html`

#### `/picks`
- **Method**: GET
- **Response**: HTML (Jinja2 template)
- **Purpose**: Trading picks page (CSP and CC)
- **Data**: 
  - CSP picks from `v_latest_run_csp_picks` view
  - CC picks from `v_latest_run_cc_picks` view
- **Template**: `templates/picks.html`











## Data Fetching

### Helper Function: `_safe_select()`

**Purpose**: Safely query Supabase views with error handling.

**Signature**: `_safe_select(view_name: str, limit: int = 100) -> tuple[List[Dict], bool]`

**Returns**:
- `(data, has_error)` tuple
- `data`: List of dictionaries (empty list on error)
- `has_error`: True if query failed, False if successful

**Error Handling**:
- Catches all exceptions
- Logs error with full details
- Checks for "relation does not exist" error (view missing)
- Returns empty list and `has_error=True` on failure

**Usage**:
```python
candidates, has_error = _safe_select("v_latest_run_top25_candidates", limit=25)
```

### Data Source

**Supabase Views**:
- All data comes from database views (not tables directly)
- Views are read-only (safe for dashboard)
- Views are optimized for dashboard queries

**Views Used**:
- `v_run_history` - Run history
- `v_latest_run_top25_candidates` - Top 25 candidates
- `v_latest_run_csp_picks` - CSP picks
- `v_latest_run_cc_picks` - CC picks

**Client**: `wheel/clients/supabase_client.select_all()`











## Templates

### Base Template: `templates/base.html`

**Structure**:
- HTML5 document structure
- Header with navigation
- Main content area
- Footer (optional)

**Navigation**:
- Links to: `/`, `/runs`, `/candidates`, `/picks`
- Active page highlighting (if implemented)

**CSS**: Links to `/static/style.css`

### Home Template: `templates/index.html`

**Sections**:
- Summary tiles (latest run info)
- Latest candidates table (top 25)
- Latest CSP picks table
- Latest CC picks table

**Error Handling**:
- Shows "View not found" message if `has_error=True`
- Shows "No data found" if view exists but empty

### Runs Template: `templates/runs.html`

**Content**:
- Table of run history
- Columns: run_id, run_ts, status, universe_size, candidates_count, picks_count, build_sha, notes
- Sorted by run_ts DESC (latest first)

### Candidates Template: `templates/candidates.html`

**Content**:
- Table of top 25 candidates
- Columns: ticker, score, rank, price, market_cap, sector, industry, beta, rsi, sentiment_score
- **Displays**:
  - `earn_in_days`: Days until earnings (or "N/A" if unknown)
  - `rsi`: RSI value used in scoring (or "N/A" if missing)
  - Which rule excluded a candidate (if applicable, via `reasons` field in metrics)
- Sorted by score DESC (highest first)

### Picks Template: `templates/picks.html`

**Content**:
- Two tables: CSP picks and CC picks
- Columns: ticker, action, expiration, dte, strike, premium, delta, annualized_yield
- **Displays**:
  - `earn_in_days`: Days until earnings (or "N/A" if unknown)
  - `rsi`: RSI value used in candidate scoring (or "N/A" if missing)
  - `pick_metrics`: Additional metadata including which DTE window was used
- Sorted by annualized_yield DESC (highest first)











## Static Files

### CSS: `apps/dashboard/static/style.css`

**Purpose**: Basic styling for dashboard

**Features**:
- Responsive design (mobile-friendly)
- Table styling
- Navigation styling
- Error message styling

**Note**: Basic styling (can be enhanced)











## Error Handling

### View Not Found
- **Detection**: `has_error=True` from `_safe_select()`
- **Display**: "⚠️ View not found; run migrations to create [view_name]."
- **Cause**: Migration not applied or view doesn't exist
- **Fix**: Run `make db-push` to apply migrations

### No Data Found
- **Detection**: `has_error=False` but `len(data) == 0`
- **Display**: "No data found" or empty table
- **Cause**: View exists but no data (normal for new runs)
- **Fix**: Wait for next screening run

### Database Connection Errors
- **Detection**: Exception in `_safe_select()`
- **Display**: Error message in template
- **Cause**: Supabase connection issue or invalid credentials
- **Fix**: Check `SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` environment variables











## Environment Configuration

### Local Development
- **Environment File**: `.env.local` (if exists)
- **Loading**: Conditional (`if env_file.exists()`)
- **Purpose**: Load environment variables for local testing

### Production (Render)
- **Environment Variables**: Set in Render dashboard
- **No .env.local**: Dashboard doesn't require `.env.local` on Render
- **Loading**: Relies on Render-provided environment variables











## Deployment

### Render Configuration
- **Service Type**: `web`
- **Start Command**: `PYTHONPATH=/opt/render/project/src uvicorn apps.dashboard.app:app --host 0.0.0.0 --port $PORT`
- **Port**: Uses `$PORT` environment variable (Render-provided)
- **Health Check**: `/health` endpoint

### Local Development
```bash
PYTHONPATH=. uvicorn apps.dashboard.app:app --reload
```
- Access at: `http://localhost:8000`
- Auto-reload on file changes (development mode)











## Extending the Dashboard

### Adding New Routes
1. Add route function in `app.py`
2. Create template in `templates/`
3. Query data using `_safe_select()` or `select_all()`
4. Render template with data

### Adding New Views
1. Create view in Supabase migration
2. Query view using `_safe_select()` in route
3. Display data in template

### Styling
- Edit `apps/dashboard/static/style.css`
- Add new CSS classes as needed
- Consider responsive design for mobile











## Known Issues

### View Error Detection
- **Issue**: `_safe_select()` may return `has_error=True` even when view exists but is empty
- **Workaround**: Templates check both `has_error` and `len(data) == 0`
- **Status**: Handled in current implementation

### Timezone Display
- **Issue**: Timestamps displayed in UTC (not converted to local time)
- **Future**: Could add timezone conversion in templates
- **Status**: Acceptable for now (all data in UTC)

### No Authentication
- **Issue**: Dashboard is publicly accessible (no auth)
- **Future**: Could add basic auth or OAuth
- **Status**: Acceptable for internal use (Render URL not publicized)











## Related Files

- `apps/dashboard/app.py` - Main application
- `apps/dashboard/templates/*.html` - Jinja2 templates
- `apps/dashboard/static/style.css` - CSS styling
- `wheel/clients/supabase_client.py` - Database client
- `supabase/migrations/20251231170000_dashboard_v1_views.sql` - Dashboard views
- `Documentation/database.md` - Database views documentation


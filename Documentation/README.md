# Documentation Index

This folder contains comprehensive documentation for the Wheel System codebase.

## Purpose

These documentation files serve two main purposes:

1. **For Project Managers**: Understand what the system does, how it works, and where things are located without diving into code details.

2. **For LLM Assistants**: Get familiar with the codebase quickly, understand implementation specifics, workarounds, and quirks to save investigation time in future chat sessions.

## Documentation Structure

### Main README
- **Location**: `README.md` (repository root)
- **Contents**: Project overview, technology stack, quick start guide
- **Audience**: Anyone new to the project

### Core Documentation Files

#### overview.md
**System architecture and high-level flows**
- Major components and their roles
- Data flow diagrams
- Key design decisions
- File organization

#### authFlow.md
**Authentication and authorization**
- FMP API key authentication
- Schwab OAuth 2.0 flow
- Token management and refresh
- Security considerations
- Common authentication issues

#### dataFlow.md
**Detailed data processing workflows**
- Weekly screening step-by-step
- RSI snapshot process
- CSP pick generation flow
- CC pick generation flow
- Daily tracker flow
- Data dependencies and relationships

#### apiClients.md
**External API integrations**
- FMP Stable client implementation
- Schwab Trader API client
- Schwab Market Data API client
- Supabase database client
- Error handling patterns
- Rate limiting and retries
- Known API quirks

#### database.md
**Database schema and management**
- Core tables and their purposes
- Database views for dashboard
- Migration strategy
- Indexes and constraints
- Common queries
- Data relationships

#### deployment.md
**Infrastructure and deployment**
- Render.com configuration
- Cron job schedules
- Environment variables
- Deployment process
- Monitoring and troubleshooting
- Timezone handling

#### dashboard.md
**Web application documentation**
- FastAPI application structure
- Routes and endpoints
- Template system
- Data fetching
- Error handling
- Extending the dashboard

#### issues.md
**Known issues and technical debt**
- Critical issues
- Known bugs and workarounds
- Technical debt items
- Code quality issues
- Performance considerations
- Security considerations
- Recommendations for improvements

## How to Use This Documentation

### For New Developers
1. Start with `README.md` for project overview
2. Read `overview.md` for system architecture
3. Read relevant section docs based on what you're working on
4. Check `issues.md` for known problems before debugging

### For Project Managers
1. Read `README.md` for high-level understanding
2. Read `overview.md` for system flows
3. Read `deployment.md` for infrastructure details
4. Check `issues.md` for current problems and priorities

### For LLM Assistants
1. Read `overview.md` to understand system structure
2. Read relevant section docs for specific areas
3. **Always check `issues.md`** for workarounds and quirks before suggesting fixes
4. Reference code locations mentioned in docs

## Maintaining Documentation

### When to Update

**Update documentation when**:
- Adding new features or components
- Changing architecture or data flows
- Discovering new issues or workarounds
- Fixing known issues (remove from `issues.md`)
- Changing deployment configuration
- Adding new API integrations

### How to Update

1. **Edit the relevant `.md` file** in `Documentation/` folder
2. **Maintain 10-line breaks** between major sections (## headings)
3. **Keep it project-manager friendly** (not too technical, but clear)
4. **Reference code locations** (file paths, function names)
5. **Document workarounds and quirks** in `issues.md`
6. **Commit with descriptive message**: `docs: update [section] documentation`

### Documentation Standards

- **Format**: Markdown (.md files)
- **Spacing**: 10 blank lines between major sections (## headings)
- **Code References**: Use backticks for file paths and function names
- **Tone**: Professional but accessible (project manager level)
- **Completeness**: Include enough detail to understand without reading code

## File Locations Reference

### Code Structure
- **Workers**: `apps/worker/src/*.py`
- **Dashboard**: `apps/dashboard/*.py`
- **API Clients**: `wheel/clients/*.py`
- **Database**: `supabase/migrations/*.sql`
- **Config**: `render.yaml`, `requirements.txt`, `runtime.txt`

### Key Files
- **Main Screener**: `apps/worker/src/weekly_screener.py`
- **RSI Cache**: `apps/worker/src/rsi_snapshot.py`
- **CSP Picks**: `apps/worker/src/build_csp_picks.py`
- **CC Picks**: `apps/worker/src/build_cc_picks.py`
- **Daily Tracker**: `apps/worker/src/daily_tracker.py`
- **Dashboard**: `apps/dashboard/app.py`
- **FMP Client**: `wheel/clients/fmp_stable_client.py`
- **Schwab Client**: `wheel/clients/schwab_client.py`
- **Supabase Client**: `wheel/clients/supabase_client.py`

## Quick Reference

### Common Tasks

**Run a worker locally**:
```bash
PYTHONPATH=. python -m apps.worker.src.weekly_screener
```

**Apply database migrations**:
```bash
make db-push
```

**Test database connection**:
```bash
make db-smoke
```

**Run dashboard locally**:
```bash
PYTHONPATH=. uvicorn apps.dashboard.app:app --reload
```

**Check cron schedules**:
See `render.yaml` or `Documentation/deployment.md`

**Find API client code**:
See `Documentation/apiClients.md` for file locations

**Understand data flow**:
See `Documentation/dataFlow.md` for step-by-step processes

**Debug known issues**:
See `Documentation/issues.md` for workarounds and quirks

## Contributing

When adding new features:
1. Update relevant documentation files
2. Add any new issues/workarounds to `issues.md`
3. Update this README if adding new documentation files
4. Maintain 10-line breaks between sections
5. Keep documentation in sync with code changes

## Questions?

If documentation is unclear or missing information:
1. Check `issues.md` for known problems
2. Review relevant section documentation
3. Check code comments in source files
4. Update documentation after finding the answer (help future readers)


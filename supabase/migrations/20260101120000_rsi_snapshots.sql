-- ============================================================================
-- RSI Snapshots Table
-- ============================================================================
-- Cache for RSI values fetched from Alpha Vantage API
-- Updated daily by rsi_snapshot.py worker

create table if not exists rsi_snapshots (
    id uuid primary key default gen_random_uuid(),
    ticker text not null,
    as_of_date date not null,
    interval text not null,
    period integer not null,
    rsi numeric,
    source text default 'alpha_vantage',
    created_at timestamptz not null default now(),
    unique(ticker, as_of_date, interval, period)
);

-- Indexes for efficient lookups
create index if not exists idx_rsi_snapshots_ticker on rsi_snapshots(ticker);
create index if not exists idx_rsi_snapshots_as_of_date_desc on rsi_snapshots(as_of_date desc);
create index if not exists idx_rsi_snapshots_ticker_date on rsi_snapshots(ticker, as_of_date desc);


-- ============================================================================
-- IV Snapshots Table
-- ============================================================================
-- Cache for implied volatility (IV) values fetched from Schwab option chains
-- Updated daily by iv_snapshot.py worker
-- Used by weekly_screener to compute IV Rank, IV Percentile, and IV Z-Score

create table if not exists iv_snapshots (
    id uuid primary key default gen_random_uuid(),
    symbol text not null,
    asof_date date not null,
    exp_date date not null,
    dte int not null,
    strike numeric not null,
    underlying_price numeric not null,
    iv numeric not null,
    source text not null default 'schwab',
    created_at timestamptz not null default now(),
    unique(symbol, asof_date)
);

-- Indexes for efficient lookups
create index if not exists idx_iv_snapshots_symbol on iv_snapshots(symbol);
create index if not exists idx_iv_snapshots_asof_date_desc on iv_snapshots(asof_date desc);
create index if not exists idx_iv_snapshots_symbol_date on iv_snapshots(symbol, asof_date desc);

-- Comment
comment on table iv_snapshots is 'Daily snapshots of implied volatility (IV) from Schwab option chains. Used to compute IV Rank, Percentile, and Z-Score for screening candidates.';


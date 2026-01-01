-- Enable uuid extension if not already enabled
create extension if not exists "pgcrypto";

-- ============================================================================
-- screening_candidates table
-- ============================================================================
create table if not exists screening_candidates (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references screening_runs(run_id) on delete cascade,
    ticker text not null,
    score numeric,
    rank integer,
    price numeric,
    market_cap numeric,
    sector text,
    industry text,
    iv numeric,
    iv_rank numeric,
    beta numeric,
    rsi numeric,
    earn_in_days integer,
    sentiment_score numeric,
    metrics jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(run_id, ticker)
);

-- Indexes for screening_candidates
create index if not exists idx_screening_candidates_run_id on screening_candidates(run_id);
create index if not exists idx_screening_candidates_ticker on screening_candidates(ticker);
create index if not exists idx_screening_candidates_score_desc on screening_candidates(score desc nulls last);

-- ============================================================================
-- screening_picks table
-- ============================================================================
create table if not exists screening_picks (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references screening_runs(run_id) on delete cascade,
    ticker text not null,
    action text not null,
    dte integer,
    target_delta numeric,
    expiration date,
    strike numeric,
    premium numeric,
    annualized_yield numeric,
    delta numeric,
    score numeric,
    rank integer,
    price numeric,
    iv numeric,
    iv_rank numeric,
    beta numeric,
    rsi numeric,
    earn_in_days integer,
    sentiment_score numeric,
    pick_metrics jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique(run_id, ticker, action)
);

-- Indexes for screening_picks
create index if not exists idx_screening_picks_run_id on screening_picks(run_id);
create index if not exists idx_screening_picks_action on screening_picks(action);
create index if not exists idx_screening_picks_annualized_yield_desc on screening_picks(annualized_yield desc nulls last);

-- Add missing columns if they don't exist (for tables created before this migration)
do $$
begin
    -- Add expiration column if missing
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_picks' and column_name = 'expiration'
    ) then
        alter table screening_picks add column expiration date;
    end if;
    
    -- Add delta column if missing
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_picks' and column_name = 'delta'
    ) then
        alter table screening_picks add column delta numeric;
    end if;
end $$;

-- ============================================================================
-- Dashboard Views
-- ============================================================================

-- View: Latest run top 25 candidates
create or replace view v_latest_run_top25_candidates as
select
    sc.run_id,
    sc.ticker,
    sc.score,
    sc.rank,
    sc.price,
    sc.iv,
    sc.iv_rank,
    sc.beta,
    sc.rsi,
    sc.earn_in_days,
    sc.sentiment_score
from screening_candidates sc
where sc.run_id = (
    select run_id
    from screening_runs
    order by run_ts desc
    limit 1
)
order by sc.score desc nulls last
limit 25;

-- View: Latest run CSP picks
create or replace view v_latest_run_csp_picks as
select
    sp.run_id,
    sp.ticker,
    sp.action,
    sp.dte,
    sp.target_delta,
    sp.expiration,
    sp.strike,
    sp.premium,
    sp.annualized_yield,
    sp.delta,
    sp.score,
    sp.rank,
    sp.price,
    sp.iv,
    sp.iv_rank,
    sp.beta,
    sp.rsi,
    sp.earn_in_days,
    sp.sentiment_score,
    sp.pick_metrics,
    sp.created_at,
    sp.updated_at
from screening_picks sp
where sp.run_id = (
    select run_id
    from screening_runs
    order by run_ts desc
    limit 1
)
  and sp.action = 'CSP'
order by sp.annualized_yield desc nulls last;


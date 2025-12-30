-- Broker account snapshots (Schwab, etc.)
create table if not exists broker_snapshots (
    id uuid primary key default gen_random_uuid(),
    ts timestamptz not null,
    source text not null,  -- 'schwab', etc.
    account_id text not null,
    cash numeric,
    net_liquidation numeric,
    positions jsonb default '[]'::jsonb,
    raw jsonb,
    created_at timestamptz default now()
);

create index if not exists idx_broker_snapshots_ts on broker_snapshots (ts desc);
create index if not exists idx_broker_snapshots_account on broker_snapshots (source, account_id, ts desc);


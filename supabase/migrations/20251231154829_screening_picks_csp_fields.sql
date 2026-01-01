-- ============================================================================
-- Ensure screening_picks has all required CSP fields
-- ============================================================================

-- Add expiration column if missing
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_picks' and column_name = 'expiration'
    ) then
        alter table screening_picks add column expiration date;
    end if;
end $$;

-- Add delta column if missing
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_picks' and column_name = 'delta'
    ) then
        alter table screening_picks add column delta numeric;
    end if;
end $$;

-- Add target_delta column if missing
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_picks' and column_name = 'target_delta'
    ) then
        alter table screening_picks add column target_delta numeric;
    end if;
end $$;

-- Add dte column if missing
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_picks' and column_name = 'dte'
    ) then
        alter table screening_picks add column dte integer;
    end if;
end $$;

-- Add strike column if missing
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_picks' and column_name = 'strike'
    ) then
        alter table screening_picks add column strike numeric;
    end if;
end $$;

-- Add premium column if missing
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_picks' and column_name = 'premium'
    ) then
        alter table screening_picks add column premium numeric;
    end if;
end $$;

-- Add annualized_yield column if missing
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_picks' and column_name = 'annualized_yield'
    ) then
        alter table screening_picks add column annualized_yield numeric;
    end if;
end $$;

-- Add pick_metrics column if missing
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_picks' and column_name = 'pick_metrics'
    ) then
        alter table screening_picks add column pick_metrics jsonb;
    end if;
end $$;

-- Ensure unique constraint exists (run_id, ticker, action)
-- Note: This will fail if constraint already exists with different name, but that's OK
-- Supabase/Postgres will handle it gracefully in most cases
do $$
begin
    if not exists (
        select 1 from pg_constraint
        where conrelid = 'screening_picks'::regclass
        and conname = 'screening_picks_run_id_ticker_action_key'
    ) then
        -- Try to create unique constraint (may fail if already exists with different name)
        begin
            alter table screening_picks add constraint screening_picks_run_id_ticker_action_key 
                unique (run_id, ticker, action);
        exception when duplicate_table then
            -- Constraint already exists with different name, that's fine
            null;
        end;
    end if;
end $$;


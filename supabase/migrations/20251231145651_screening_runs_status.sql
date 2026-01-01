-- ============================================================================
-- Add status tracking columns to screening_runs
-- ============================================================================

-- Add status column (default 'running' for existing rows, but new rows will use default)
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_runs' and column_name = 'status'
    ) then
        alter table screening_runs add column status text not null default 'running';
    end if;
end $$;

-- Add error column
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_runs' and column_name = 'error'
    ) then
        alter table screening_runs add column error text;
    end if;
end $$;

-- Add candidates_count column
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_runs' and column_name = 'candidates_count'
    ) then
        alter table screening_runs add column candidates_count integer;
    end if;
end $$;

-- Add picks_count column
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_runs' and column_name = 'picks_count'
    ) then
        alter table screening_runs add column picks_count integer;
    end if;
end $$;

-- Add build_sha column
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_runs' and column_name = 'build_sha'
    ) then
        alter table screening_runs add column build_sha text;
    end if;
end $$;

-- Add finished_at column
do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_name = 'screening_runs' and column_name = 'finished_at'
    ) then
        alter table screening_runs add column finished_at timestamptz;
    end if;
end $$;

-- Add constraint to ensure status is one of the allowed values
do $$
begin
    if not exists (
        select 1 from information_schema.constraint_column_usage
        where table_name = 'screening_runs' and constraint_name = 'screening_runs_status_check'
    ) then
        alter table screening_runs add constraint screening_runs_status_check 
            check (status in ('running', 'success', 'failed'));
    end if;
end $$;

-- Add index on run_ts desc (for querying latest runs)
create index if not exists idx_screening_runs_run_ts_desc on screening_runs(run_ts desc);

-- Add index on status (for filtering by status)
create index if not exists idx_screening_runs_status on screening_runs(status);


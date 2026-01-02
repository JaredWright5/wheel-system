-- ============================================================================
-- Dashboard Views v1
-- ============================================================================

-- View: Run history (last 200 runs)
create or replace view v_run_history as
select
    sr.run_id,
    sr.run_ts,
    sr.status,
    sr.universe_size,
    sr.candidates_count,
    sr.picks_count,
    sr.build_sha,
    sr.notes
from screening_runs sr
order by sr.run_ts desc
limit 200;

-- View: Latest run top 25 candidates (matching existing column order)
drop view if exists v_latest_run_top25_candidates;
create view v_latest_run_top25_candidates as
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
    where status = 'success'
      and notes != 'DAILY_TRACKER'
    order by run_ts desc
    limit 1
)
order by sc.score desc nulls last
limit 25;

-- View: Latest run CSP picks (matching existing column order)
drop view if exists v_latest_run_csp_picks;
create view v_latest_run_csp_picks as
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

-- View: Latest run CC picks
create or replace view v_latest_run_cc_picks as
select
    sp.run_id,
    sp.ticker,
    sp.action,
    sp.expiration,
    sp.dte,
    sp.target_delta,
    sp.strike,
    sp.premium,
    sp.delta,
    sp.annualized_yield,
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
    where status = 'success'
      and notes != 'DAILY_TRACKER'
    order by run_ts desc
    limit 1
)
  and sp.action = 'CC'
order by sp.annualized_yield desc nulls last;

-- View: Latest run all picks (CSP + CC combined)
create or replace view v_latest_run_all_picks as
select
    sp.run_id,
    sp.ticker,
    sp.action,
    sp.expiration,
    sp.dte,
    sp.target_delta,
    sp.strike,
    sp.premium,
    sp.delta,
    sp.annualized_yield,
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
order by
    case when sp.action = 'CSP' then 0 else 1 end,  -- CSP first, then CC
    sp.annualized_yield desc nulls last;

-- ============================================================================
-- Helpful Indexes (create only if missing)
-- ============================================================================

-- Index on screening_runs(run_ts desc)
create index if not exists idx_screening_runs_run_ts_desc on screening_runs(run_ts desc);

-- Index on screening_picks(run_id)
create index if not exists idx_screening_picks_run_id on screening_picks(run_id);

-- Index on screening_picks(action)
create index if not exists idx_screening_picks_action on screening_picks(action);

-- Index on screening_candidates(run_id)
create index if not exists idx_screening_candidates_run_id on screening_candidates(run_id);

-- Index on screening_candidates(score desc)
create index if not exists idx_screening_candidates_score_desc on screening_candidates(score desc nulls last);


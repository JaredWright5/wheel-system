-- ============================================================================
-- Fix Dashboard Views to Filter for Successful Screening Runs
-- ============================================================================
-- This migration updates all 'latest run' views to only select from
-- successful screening runs (status='success' and notes!='DAILY_TRACKER')
-- instead of just the latest run by timestamp (which could be a daily tracker)

-- View: Latest run top 25 candidates
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

-- View: Latest run CSP picks
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
    where status = 'success'
      and notes != 'DAILY_TRACKER'
    order by run_ts desc
    limit 1
)
  and sp.action = 'CSP'
order by sp.annualized_yield desc nulls last;

-- View: Latest run CC picks
drop view if exists v_latest_run_cc_picks;
create view v_latest_run_cc_picks as
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
drop view if exists v_latest_run_all_picks;
create view v_latest_run_all_picks as
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
order by
    case when sp.action = 'CSP' then 0 else 1 end,  -- CSP first, then CC
    sp.annualized_yield desc nulls last;


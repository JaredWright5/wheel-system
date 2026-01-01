-- ============================================================================
-- View: Latest run all picks (CSP + CC combined)
-- ============================================================================

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


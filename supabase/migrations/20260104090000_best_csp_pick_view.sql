-- Best CSP pick for the latest successful run (based on JSON marker set by build_csp_picks)
create or replace view public.v_latest_run_best_csp_pick as
with latest_run as (
  select run_id
  from public.screening_runs
  where status = 'success'
  order by run_ts desc
  limit 1
)
select
  sp.*
from public.screening_picks sp
join latest_run lr on lr.run_id = sp.run_id
where sp.action = 'CSP'
  and coalesce((sp.pick_metrics->'trade_card'->>'best_of_run')::boolean, false) = true
limit 1;

-- Helpful indexes already exist on screening_picks.run_id/action; JSON filter is small (latest run only).


-- =========================
-- Dashboard views + approved universe
-- =========================

-- Latest Top 25 (latest run)
create or replace view latest_wheel_top25 as
select
  wc.run_id,
  sr.run_ts,
  wc.ticker,
  t.name,
  t.sector,
  t.industry,
  t.market_cap,
  wc.wheel_score,
  wc.score_fundamentals,
  wc.score_trend,
  wc.score_events,
  wc.features
from wheel_candidates wc
join screening_runs sr on sr.run_id = wc.run_id
left join tickers t on t.ticker = wc.ticker
where wc.run_id = (
  select run_id
  from screening_runs
  order by run_ts desc
  limit 1
)
order by wc.wheel_score desc
limit 25;

-- Run history summary
create or replace view wheel_run_history as
select
  sr.run_id,
  sr.run_ts,
  sr.universe_size,
  count(wc.ticker) as candidate_count,
  avg(wc.wheel_score)::numeric(5,2) as avg_wheel_score,
  max(wc.wheel_score) as max_wheel_score,
  min(wc.wheel_score) as min_wheel_score
from screening_runs sr
left join wheel_candidates wc on wc.run_id = sr.run_id
group by sr.run_id, sr.run_ts, sr.universe_size
order by sr.run_ts desc;

-- Top 25 per run (Postgres-safe, no QUALIFY)
create or replace view wheel_top25_per_run as
select
  run_id,
  run_ts,
  ticker,
  wheel_score
from (
  select
    wc.run_id,
    sr.run_ts,
    wc.ticker,
    wc.wheel_score,
    row_number() over (
      partition by wc.run_id
      order by wc.wheel_score desc
    ) as rn
  from wheel_candidates wc
  join screening_runs sr on sr.run_id = wc.run_id
) ranked
where rn <= 25
order by run_ts desc, wheel_score desc;

-- Latest vs Previous deltas (shows movers, new entrants, fallen out)
create or replace view latest_wheel_deltas as
with runs as (
  select run_id, run_ts,
         row_number() over (order by run_ts desc) as rn
  from screening_runs
),
latest as (
  select run_id, run_ts from runs where rn = 1
),
prev as (
  select run_id, run_ts from runs where rn = 2
),
latest_ranked as (
  select
    wc.ticker,
    wc.wheel_score as score_latest,
    row_number() over (order by wc.wheel_score desc) as rank_latest
  from wheel_candidates wc
  join latest l on l.run_id = wc.run_id
),
prev_ranked as (
  select
    wc.ticker,
    wc.wheel_score as score_prev,
    row_number() over (order by wc.wheel_score desc) as rank_prev
  from wheel_candidates wc
  join prev p on p.run_id = wc.run_id
)
select
  l.run_ts as latest_run_ts,
  p.run_ts as prev_run_ts,
  coalesce(lr.ticker, pr.ticker) as ticker,
  lr.score_latest,
  pr.score_prev,
  lr.rank_latest,
  pr.rank_prev,
  (lr.score_latest - pr.score_prev) as score_change,
  (pr.rank_prev - lr.rank_latest) as rank_change,
  case
    when lr.ticker is not null and pr.ticker is null then 'NEW'
    when lr.ticker is null and pr.ticker is not null then 'DROPPED'
    else 'STAY'
  end as status
from latest_ranked lr
full outer join prev_ranked pr
  on lr.ticker = pr.ticker
cross join latest l
cross join prev p;

-- Approved universe (lock in top names each run for stability)
create table if not exists approved_universe (
  ticker text primary key references tickers(ticker),
  approved boolean default true,
  last_run_id uuid,
  last_run_ts timestamptz,
  last_rank int,
  last_score int,
  notes text,
  updated_at timestamptz default now()
);

-- Helpful index for historical queries
create index if not exists idx_wheel_candidates_run_score
  on wheel_candidates (run_id, wheel_score desc);


WITH qualified_act as (
SELECT athlete_hub_seq
  ,CONCAT(firstname," ", lastname) as athlete
  , FORMAT_TIMESTAMP("%b %Y", start_date) as event_month
  , FORMAT_TIMESTAMP("%W", start_date) as event_week
  , sum(moving_time_s) as total_time_s
FROM `strava-int.strava_information_vault.fact_activity`
GROUP BY athlete_hub_seq
  ,CONCAT(firstname," ", lastname)
--   , DATE(start_date)
  , FORMAT_TIMESTAMP("%b %Y", start_date)
  , FORMAT_TIMESTAMP("%W", start_date)
)

 select athlete_hub_seq
  , athlete
  , event_month
  , event_week
  , total_time_s
  , RANK()
    OVER (PARTITION BY event_week order by total_time_s desc) as week_rank
  , CASE WHEN (RANK()
    OVER (PARTITION BY event_week order by total_time_s desc)) = 1 THEN 1 ELSE 0 END as week_points
 from qualified_act
 where total_time_s > 0

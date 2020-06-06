WITH qualified_act as (
SELECT athlete_hub_seq
  ,CONCAT(firstname," ", lastname) as athlete
  , FORMAT_TIMESTAMP("%b %Y", start_date) as event_month
  , sum(moving_time_s) as total_time_s
FROM `strava-int.strava_information_vault.fact_activity`
GROUP BY athlete_hub_seq
  ,CONCAT(firstname," ", lastname)
  , FORMAT_TIMESTAMP("%b %Y", start_date)
)

 select athlete_hub_seq
  , athlete
  , event_month
  , total_time_s
  , RANK()
    OVER (PARTITION BY event_month  order by total_time_s desc) as month_rank
  , CASE WHEN (RANK()
    OVER (PARTITION BY event_month order by total_time_s desc)) = 1 THEN 2 ELSE 0 END as month_points
 from qualified_act
 where total_time_s > 0

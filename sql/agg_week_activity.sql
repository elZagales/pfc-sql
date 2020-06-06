WITH qualified_act as (
SELECT athlete_hub_seq
  ,CONCAT(firstname," ", lastname) as athlete
  , FORMAT_TIMESTAMP("%b %Y", start_date) as event_month
  , FORMAT_TIMESTAMP("%W", start_date) as event_week
  , count(activity_hub_seq) act_count
FROM `strava-int.strava_information_vault.fact_activity`
where moving_time_s > 1500
GROUP BY athlete_hub_seq
  ,CONCAT(firstname," ", lastname)
  , FORMAT_TIMESTAMP("%b %Y", start_date)
  , FORMAT_TIMESTAMP("%W", start_date)
)

 select athlete_hub_seq
  , athlete
  , event_month
  , event_week
  , act_count
  , RANK()
    OVER (PARTITION BY event_week order by act_count desc) as week_rank
  , CASE WHEN (RANK()
    OVER (PARTITION BY event_week order by act_count desc)) = 1 THEN 1 ELSE 0 END as week_points
 from qualified_act 

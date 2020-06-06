WITH qualified_act as (
SELECT athlete_hub_seq
  ,CONCAT(firstname," ", lastname) as athlete
  , FORMAT_TIMESTAMP("%b %Y", start_date) as event_month
  , count(activity_hub_seq) act_count
FROM `strava-int.strava_information_vault.fact_activity`
where moving_time_s > 1500
GROUP BY athlete_hub_seq
  ,CONCAT(firstname," ", lastname)
  , FORMAT_TIMESTAMP("%b %Y", start_date)
)

 select athlete_hub_seq
  , athlete
  , event_month
  , act_count
  , RANK()
    OVER (PARTITION BY event_month  order by act_count desc) as month_rank
  , CASE WHEN (RANK()
    OVER (PARTITION BY event_month order by act_count desc)) = 1 THEN 2 ELSE 0 END as most_activities_points
  , CASE WHEN act_count >= 20 THEN 2 ELSE 0 END as twenty_activities_points
 from qualified_act

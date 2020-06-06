WITH _5k_act as (
SELECT athlete_hub_seq
  ,CONCAT(firstname," ", lastname) as athlete
  , event_month
  , activity_hub_seq
  , distance_mi
  , activity_type
  , average_speed_mi
  , 2 as monthly_5k
  , RANK() OVER (PARTITION BY event_month
    ORDER BY average_speed_mi DESC) as fastest_5k_rank
  , CASE WHEN (RANK() OVER (PARTITION BY event_month
    ORDER BY average_speed_mi DESC)) = 1 then 1 else 0 end as fastest_monthly_5k
FROM `strava-int.strava_information_vault.fact_activity`
where distance_mi >= 3.08
and lower(activity_name) like '%pandera monthly 5k%'
),
 month_qualified_act as (
SELECT athlete_hub_seq
  , CONCAT(firstname," ", lastname) as athlete
  , event_month
  , count(activity_hub_seq) act_count
  , RANK()
    OVER (PARTITION BY event_month  order by count(activity_hub_seq) desc) as month_rank
  , CASE WHEN (RANK()
    OVER (PARTITION BY event_month order by count(activity_hub_seq) desc)) = 1 THEN 2 ELSE 0 END as month_activity_points
  , CASE WHEN count(activity_hub_seq) >= 20 THEN 2 ELSE 0 END as twenty_activities_points
FROM `strava-int.strava_information_vault.fact_activity`
WHERE moving_time_s > 1500
GROUP BY athlete_hub_seq
  ,CONCAT(firstname," ", lastname)
  , event_month
),
 month_qualified_time as (
SELECT athlete_hub_seq
  , CONCAT(firstname," ", lastname) as athlete
  , event_month
  , sum(moving_time_s) as total_time_s
  , RANK()
  OVER (PARTITION BY event_month  order by sum(moving_time_s) desc) as month_rank
, CASE WHEN (RANK()
  OVER (PARTITION BY event_month order by sum(moving_time_s) desc)) = 1 THEN 2 ELSE 0 END as month_time_points
FROM `strava-int.strava_information_vault.fact_activity`
GROUP BY athlete_hub_seq
  , athlete
  , event_month
HAVING sum(moving_time_s) > 0
),
 week_qualified_act as (
SELECT athlete_hub_seq
  , CONCAT(firstname," ", lastname) as athlete
  , event_month
  , event_week
  , count(activity_hub_seq) act_count
  , RANK()
    OVER (PARTITION BY event_week order by count(activity_hub_seq) desc) as week_rank
  , CASE WHEN (RANK()
    OVER (PARTITION BY event_week order by count(activity_hub_seq) desc)) = 1 THEN 1 ELSE 0 END as week_activity_points
FROM `strava-int.strava_information_vault.fact_activity`
where moving_time_s > 1500
GROUP BY athlete_hub_seq
  ,CONCAT(firstname," ", lastname)
  , event_month
  , event_week
),
 week_qualified_time as (
SELECT athlete_hub_seq
  , CONCAT(firstname," ", lastname) as athlete
  , event_month
  , event_week
  , sum(moving_time_s) as total_time_s
  , RANK()
    OVER (PARTITION BY event_week order by sum(moving_time_s) desc) as week_rank
  , CASE WHEN (RANK()
    OVER (PARTITION BY event_week order by sum(moving_time_s) desc)) = 1 THEN 1 ELSE 0 END as week_time_points
FROM `strava-int.strava_information_vault.fact_activity`
GROUP BY athlete_hub_seq
  , athlete
  , event_month
  , event_week
HAVING sum(moving_time_s) > 0
),
 week_agg as (
   SELECT COALESCE(wqa.athlete_hub_seq, wqt.athlete_hub_seq) as athlete_hub_seq
     , COALESCE(wqa.athlete, wqt.athlete) as athlete
     , COALESCE(wqa.event_month, wqt.event_month) as event_month
     , sum(wqa.week_activity_points) as week_activity_points
     , sum(wqt.week_time_points) as week_time_points
   FROM week_qualified_act wqa
   JOIN week_qualified_time wqt
     ON wqa.athlete_hub_seq = wqt.athlete_hub_seq
       AND wqa.athlete = wqt.athlete
       AND wqa.event_month = wqt.event_month
       AND wqa.event_week = wqt.event_week
   GROUP BY athlete_hub_seq
     , athlete
     , event_month
 )

SELECT COALESCE(fka.athlete_hub_seq, mqa.athlete_hub_seq, mqt.athlete_hub_seq, wa.athlete_hub_seq) as athlete_hub_seq
  , COALESCE(fka.athlete, mqa.athlete, mqt.athlete, wa.athlete) as athlete
  , COALESCE(fka.event_month, mqa.event_month, mqt.event_month, wa.event_month) as event_month
  , sum(mqa.month_activity_points+mqa.twenty_activities_points+mqt.month_time_points+wa.week_activity_points+wa.week_time_points+ifnull(fka.monthly_5k,0)+ifnull(fka.fastest_monthly_5k,0)) as total_points
  , ifnull(sum(fka.monthly_5k),0) as monthly_5k
  , ifnull(sum(fka.fastest_monthly_5k),0) as fastest_monthly_5k
  , sum(mqa.month_activity_points) as month_activity_points
  , sum(mqa.twenty_activities_points) as twenty_activities_points
  , sum(mqt.month_time_points) as month_time_points
  , sum(wa.week_activity_points) as week_activity_points
  , sum(wa.week_time_points) as week_time_points
FROM month_qualified_act mqa
FULL OUTER JOIN _5k_act fka
  ON fka.athlete_hub_seq = mqa.athlete_hub_seq
    AND fka.athlete = mqa.athlete
    AND fka.event_month = mqa.event_month
FULL OUTER JOIN month_qualified_time mqt
  ON mqa.athlete_hub_seq = mqt.athlete_hub_seq
    AND mqa.athlete = mqt.athlete
    AND mqa.event_month = mqt.event_month
FULL OUTER JOIN week_agg wa
  ON mqa.athlete_hub_seq = wa.athlete_hub_seq
    AND mqa.athlete = wa.athlete
    AND mqa.event_month = wa.event_month
GROUP BY athlete_hub_seq
  , athlete
  , event_month

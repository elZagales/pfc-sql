WITH _5k_act as (
SELECT athlete_hub_seq
  ,CONCAT(firstname," ", lastname) as athlete
  , FORMAT_TIMESTAMP("%b %Y", start_date) as event_month
  , activity_hub_seq
  , distance_mi
  , activity_type
  , average_speed_mi
  , 2 as monthly_5k
FROM `strava-int.strava_information_vault.fact_activity`
where distance_mi >= 3.08
and lower(activity_name) like '%pandera monthly 5k%'
)

select distinct fa.athlete_hub_seq
  , CONCAT(fa.firstname," ", fa.lastname) as athlete
  , FORMAT_TIMESTAMP("%b %Y", fa.start_date) as event_month
  , fka.activity_hub_seq
  , fka.distance_mi
  , fka.activity_type
  , fka.average_speed_mi
  , COALESCE(fka.monthly_5k, 0) as monthly_5k
  , RANK()
    OVER (PARTITION BY FORMAT_TIMESTAMP("%b %Y", fa.start_date) order by fka.average_speed_mi desc) as month_rank
   , CASE WHEN (RANK()
    OVER (PARTITION BY FORMAT_TIMESTAMP("%b %Y", fa.start_date) order by fka.average_speed_mi desc)) = 1 then 1 else 0 end as fastest_monthly_5k
from `strava-int.strava_information_vault.fact_activity` fa
left join _5k_act fka
 on fa.athlete_hub_seq = fka.athlete_hub_seq
 and FORMAT_TIMESTAMP("%b %Y", fa.start_date) = fka.event_month
order by average_speed_mi asc

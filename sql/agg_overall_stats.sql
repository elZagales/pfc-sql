SELECT  athlete_hub_seq
  , CONCAT( firstname, ' ', lastname) as athlete
  , activity_type
  , FORMAT_TIMESTAMP("%b %Y", start_date) as event_month
  , sum(moving_time_s/60) activity_minutes
  , sum(distance_mi) distance_mi
  , sum( total_elevation_gain_m) elev_gain_m
  , sum(kilojoules) kilojoules
  , sum(calories) calories
FROM `strava-int.strava_information_vault.fact_activity`
WHERE delete_ind = FALSE
GROUP by athlete_hub_seq
  , CONCAT( firstname, ' ', lastname)
  , activity_type
  , FORMAT_TIMESTAMP("%b %Y", start_date)

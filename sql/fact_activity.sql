WITH act_sat_latest as (
  SELECT activity_hub_seq,
    MAX(sat_load_date) as latest_load_date
  FROM strava-int.strava_datavault.activity_sat
  GROUP BY activity_hub_seq)

SELECT DISTINCT ath_s.athlete_hub_seq
  , ath_s.firstname
  , ath_s.lastname
  , FORMAT_TIMESTAMP("%b %Y", act_s.start_date) as event_month
  , FORMAT_TIMESTAMP("%W", start_date) as event_week
  , act_s.*
FROM act_sat_latest asl
JOIN strava-int.strava_datavault.activity_sat act_s
  on (asl.activity_hub_seq = act_s.activity_hub_seq
      and asl.latest_load_date = act_s.sat_load_date)
JOIN `strava-int.strava_datavault.athlete_activity_link` aal
  on act_s.activity_hub_seq = aal.activity_hub_seq
JOIN `strava-int.strava_datavault.athlete_sat` ath_s
  on aal.athlete_hub_seq = ath_s.athlete_hub_seq
WHERE act_s.delete_ind = False

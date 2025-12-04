-- tables to be loaded in Power BI for Dashboard
CREATE OR REPLACE TABLE fleet_db.analytics.trip_map AS
SELECT
  t.trip_id,
  t.driver_id,
  t.day,
  MIN(c.latitude)    AS start_lat,    -- or use first/last depending on desired point
  MIN(c.longitude)   AS start_lon,
  t.max_route_deviation,
  t.any_anomalous_event,
  t.any_route_anomaly,
  t.any_geofencing_violation
FROM fleet_db.analytics.trip_summary t
JOIN fleet_db.clean.trips_clean c USING (trip_id)
GROUP BY t.trip_id, t.driver_id, t.day, t.max_route_deviation,
         t.any_anomalous_event, t.any_route_anomaly, t.any_geofencing_violation;


select * from fleet_db.analytics.trip_map

select * from fleet_db.analytics.daily_rollup

select * from fleet_db.analytics.context_summary


CREATE OR REPLACE TABLE fleet_db.analytics.driver_risk_score AS
WITH stats AS (
  SELECT
    MIN(anomalies_count_daily) AS min_anom, 
    MAX(anomalies_count_daily) AS max_anom,
    MIN(route_anomalies_daily) AS min_ran, 
    MAX(route_anomalies_daily) AS max_ran,
    MIN(geofencing_violations_daily) AS min_geo, 
    MAX(geofencing_violations_daily) AS max_geo,
    MIN(avg_route_deviation_daily) AS min_rdev, 
    MAX(avg_route_deviation_daily) AS max_rdev,
    MIN(behavioral_consistency_index_daily) AS min_beh, 
    MAX(behavioral_consistency_index_daily) AS max_beh
  FROM fleet_db.analytics.driver_daily
),
norm AS (
  SELECT
    d.driver_id,
    d.day,
    CASE WHEN s.max_anom = s.min_anom THEN 0
         ELSE (d.anomalies_count_daily - s.min_anom) / NULLIF(s.max_anom - s.min_anom,0) END AS anom_n,
    CASE WHEN s.max_ran = s.min_ran THEN 0
         ELSE (d.route_anomalies_daily - s.min_ran) / NULLIF(s.max_ran - s.min_ran,0) END AS ran_n,
    CASE WHEN s.max_geo = s.min_geo THEN 0
         ELSE (d.geofencing_violations_daily - s.min_geo) / NULLIF(s.max_geo - s.min_geo,0) END AS geo_n,
    CASE WHEN s.max_rdev = s.min_rdev THEN 0
         ELSE (d.avg_route_deviation_daily - s.min_rdev) / NULLIF(s.max_rdev - s.min_rdev,0) END AS rdev_n,
    CASE WHEN s.max_beh = s.min_beh THEN 0
         ELSE 1 - ((d.behavioral_consistency_index_daily - s.min_beh) / NULLIF(s.max_beh - s.min_beh,0)) END AS beh_inv_n
  FROM fleet_db.analytics.driver_daily d CROSS JOIN stats s
)
SELECT
  driver_id,
  day,
  ROUND(
    0.35*beh_inv_n + 0.30*geo_n + 0.20*anom_n + 0.10*ran_n + 0.05*rdev_n
  ,4) AS risk_score_0_1,
 ROUND(
    10 * (0.35*beh_inv_n + 0.30*geo_n + 0.20*anom_n + 0.10*ran_n + 0.05*rdev_n)
  ,3) AS risk_score_1_10,
FROM norm
ORDER BY risk_score_0_1 DESC



select * from fleet_db.analytics.driver_risk_score

CREATE OR REPLACE TABLE fleet_db.analytics.trip_risk_score AS
WITH base AS (
  SELECT
    trip_id,
    driver_id,
    day,
    CASE WHEN any_anomalous_event THEN 1 ELSE 0 END AS f_anom,
    CASE WHEN any_route_anomaly THEN 1 ELSE 0 END   AS f_route,
    CASE WHEN any_geofencing_violation THEN 1 ELSE 0 END AS f_geo,
    COALESCE(max_route_deviation,0)                  AS route_dev,
    COALESCE(behavioral_consistency_index,0)         AS beh_idx
  FROM fleet_db.analytics.trip_summary
),
-- global mins / maxs for normalization
stats AS (
  SELECT
    MIN(route_dev) AS min_rdev, MAX(route_dev) AS max_rdev,
    MIN(beh_idx)  AS min_beh,  MAX(beh_idx)  AS max_beh
  FROM base
),
norm AS (
  SELECT
    b.*,
    -- flag count and mapped flag score (your chosen mapping: 0,0.33,0.67,1)
    (f_anom + f_route + f_geo) AS flag_count,
    CASE (f_anom + f_route + f_geo)
      WHEN 0 THEN 0.00
      WHEN 1 THEN 0.33
      WHEN 2 THEN 0.67
      ELSE 1.00
    END AS flag_score,
    -- normalized route deviation 0..1
    CASE WHEN s.max_rdev = s.min_rdev THEN 0
         ELSE (b.route_dev - s.min_rdev) / NULLIF(s.max_rdev - s.min_rdev,0) END AS n_route_dev,
    -- normalized inverted behavior 0..1 (higher = worse)
    CASE WHEN s.max_beh = s.min_beh THEN 0
         ELSE 1 - ((b.beh_idx - s.min_beh) / NULLIF(s.max_beh - s.min_beh,0)) END AS n_beh_inv
  FROM base b CROSS JOIN stats s
),
severity AS (
  SELECT
    *,
    -- severity is simple average of the two normalized signals
    (n_route_dev + n_beh_inv) / 2.0 AS severity_raw
  FROM norm
),
risk_scored as ( 
  SELECT
    s.trip_id,
    s.driver_id,
    s.day,
    s.flag_count,
    ROUND(s.flag_score,2)         AS flag_score,
    ROUND(s.severity_raw,4)       AS severity_0_1,
    -- raw weighted score (already 0..1)
    ROUND(0.7*s.flag_score + 0.3*s.severity_raw,4) AS risk_score_0_1,
    -- 1..10
    ROUND(10 * (0.7*s.flag_score + 0.3*s.severity_raw),3) AS risk_score_1_10,
    RANK() OVER (ORDER BY 0.7*s.flag_score + 0.3*s.severity_raw DESC) AS risk_rank
  FROM severity s
)
select *
from risk_scored
ORDER BY risk_score_0_1 DESC

select * from fleet_db.analytics.trip_risk_score


-- tables to be loaded in colab for ML 
select * from fleet_db.analytics.trip_summary

select * from fleet_db.analytics.driver_daily


CREATE OR REPLACE TABLE ANALYTICS.PREDICTIONS (
    TRIP_ID VARCHAR,
    DRIVER_ID VARCHAR,
    ACTUAL INT,
    PRED_PROB FLOAT
);


select * from fleet_db.analytics.predictions


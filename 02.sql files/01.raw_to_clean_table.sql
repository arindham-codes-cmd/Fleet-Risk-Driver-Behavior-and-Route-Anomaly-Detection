use database fleet_db

select * from fleet_db.raw.trips_raw

desc table fleet_db.raw.trips_raw


select column_name, data_type from FLEET_DB.INFORMATION_SCHEMA.COLUMNS 
where TABLE_NAME = 'TRIPS_RAW' and  TABLE_SCHEMA = 'RAW'

select 
    trip_id,
    timestamp
from fleet_db.raw.trips_raw
where try_to_timestamp(timestamp) is null

SELECT trip_id, latitude, longitude
FROM fleet_db.raw.trips_raw
WHERE TRY_TO_DOUBLE(latitude) IS NULL OR TRY_TO_DOUBLE(longitude) IS NULL

-- create trips_clean table by changing all columns to their respectie data types from original trips_raw table

CREATE OR REPLACE TABLE fleet_db.clean.trips_clean AS
SELECT
  TRIM(trip_id) AS trip_id,
  TRIM(driver_id) AS driver_id,
  TRIM(vehicle_id) AS vehicle_id,

  -- safe timestamp parse (NULL if cannot parse)
  TRY_CAST(timestamp AS TIMESTAMP) AS time_stamp,

  TRY_CAST(latitude AS DOUBLE) AS latitude,
  TRY_CAST(longitude AS DOUBLE) AS longitude,
  TRY_CAST(speed AS DOUBLE) AS speed,
  TRY_CAST(acceleration AS DOUBLE) AS acceleration,
  TRY_CAST(steering_angle AS DOUBLE) AS steering_angle,
  TRY_CAST(heading AS DOUBLE) AS heading,
  TRY_CAST(trip_duration AS DOUBLE) AS trip_duration,
  TRY_CAST(trip_distance AS DOUBLE) AS trip_distance,
  TRY_CAST(fuel_consumption AS DOUBLE) AS fuel_consumption,
  TRY_CAST(rpm AS DOUBLE) AS rpm,

  -- brake_usage is a count -> integer
  TRY_CAST(brake_usage AS INTEGER) AS brake_usage,

  TRY_CAST(lane_deviation AS DOUBLE) AS lane_deviation,

  LOWER(TRIM(weather_conditions)) AS weather_conditions,
  LOWER(TRIM(road_type)) AS road_type,
  LOWER(TRIM(traffic_condition)) AS traffic_condition,

  TRY_CAST(stop_events AS INTEGER) AS stop_events,

  -- boolean mapping from 1/0 or 'true' strings
  IFF(TRY_CAST(geofencing_violation AS INTEGER) = 1, TRUE, FALSE) AS geofencing_violation,
  IFF(TRY_CAST(anomalous_event AS INTEGER) = 1, TRUE, FALSE) AS anomalous_event,
  IFF(TRY_CAST(route_anomaly AS INTEGER) = 1, TRUE, FALSE) AS route_anomaly,

  TRY_CAST(route_deviation_score AS DOUBLE) AS route_deviation_score,
  TRY_CAST(acceleration_variation AS DOUBLE) AS acceleration_variation,
  TRY_CAST(behavioral_consistency_index AS DOUBLE) AS behavioral_consistency_index

FROM fleet_db.raw.trips_raw;

select * from fleet_db.clean.trips_clean limit 10

select count(*) from fleet_db.clean.trips_clean

select 
column_name,
data_type
from FLEET_DB.INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME = 'TRIPS_CLEAN' and TABLE_SCHEMA ='CLEAN'



select count(case when time_stamp is null then 1 end) as null_time_stamps from fleet_db.clean.trips_clean

select  min(time_stamp), max(time_stamp) from fleet_db.clean.trips_clean

select distinct
    weather_conditions,
    road_type,
    traffic_condition
from fleet_db.clean.trips_clean

select COUNT(*) from fleet_db.raw.trips_raw WHERE time_stamp IS NULL
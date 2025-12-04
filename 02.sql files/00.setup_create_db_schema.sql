-- warehouse set to the newly created GPSFLEET_WH warehouse
-- creating database 
create or replace database fleet_db

-- creating schemas inside the database 
create or replace schema fleet_db.raw;
create or replace schema fleet_db.clean;
create or replace schema fleet_db.analytics

-- type of csv format step and skipping header 
use database fleet_db

CREATE OR REPLACE FILE FORMAT csv_fmt TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

-- create stage so that I can load my csv file in this stage. 
CREATE OR REPLACE STAGE my_stage 
  FILE_FORMAT = csv_fmt;

-- check if file loaded in stage or not
list @my_stage

-- creating trips_raw table 
CREATE OR REPLACE TABLE fleet_db.raw.trips_raw (
  trip_id STRING,
  driver_id STRING,
  vehicle_id STRING,
  timestamp STRING,
  latitude STRING,
  longitude STRING,
  speed STRING,
  acceleration STRING,
  steering_angle STRING,
  heading STRING,
  trip_duration STRING,
  trip_distance STRING,
  fuel_consumption STRING,
  rpm STRING,
  brake_usage STRING,
  lane_deviation STRING,
  weather_conditions STRING,
  road_type STRING,
  traffic_condition STRING,
  stop_events STRING,
  geofencing_violation STRING,
  anomalous_event STRING,
  route_anomaly STRING,
  route_deviation_score STRING,
  acceleration_variation STRING,
  behavioral_consistency_index STRING
)


-- copy the data from stage files to the trips_raw table created 
COPY INTO fleet_db.raw.trips_raw FROM @my_stage/driver_behavior_route_anomaly_dataset_with_derived_features.csv FILE_FORMAT=(FORMAT_NAME='csv_fmt') ON_ERROR='CONTINUE';

select * from fleet_db.raw.trips_raw

SELECT COUNT(*) AS raw_rows FROM fleet_db.raw.trips_raw;



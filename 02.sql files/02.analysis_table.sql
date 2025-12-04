select * from fleet_db.clean.trips_clean


-- creating trip_summary table for analytics
create or replace table fleet_db.analytics.trip_summary as
select
    trip_id,
    driver_id,
    vehicle_id,
    date_trunc('day', time_stamp) as day,
    max(time_stamp) as trip_start_at,
    max(trip_duration)/60 as trip_duration_minutes,
    max(trip_distance) as trip_distance_Km,
    avg(speed) as avg_speed, 
    max(speed) as max_speed,
    avg(acceleration) as avg_accelaration,
    sum(brake_usage) as total_brake_events,
    max(route_deviation_score) as max_route_deviation,
    max(anomalous_event) as any_anomalous_event, 
    max(geofencing_violation) as any_geofencing_violation,
    max(route_anomaly) as any_route_anomaly,
    avg(route_deviation_score) as avg_route_deviation_score,
    avg(behavioral_consistency_index) as behavioral_consistency_index
from fleet_db.clean.trips_clean
group by trip_id, driver_id, vehicle_id, date_trunc('day', time_stamp)

-- how many trips had anomalous even and route anomaly events 
select * from fleet_db.analytics.trip_summary
where any_anomalous_event = True and any_route_anomaly = True

-- anomalos event and route anomaly events by driver
select 
driver_id,
count(distinct trip_id) as total_trips,
SUM(case when any_anomalous_event = True then 1 else 0 end) as number_of_anomalous_event,
SUM(case when any_route_anomaly = True then 1 else 0 end) as number_of_route_anomaly_event,
from fleet_db.analytics.trip_summary
group by driver_id

-- create driver daily table 
create or replace table fleet_db.analytics.driver_daily as
select
    driver_id, 
    to_date(day) as day,
    count(distinct trip_id) as trip_count_daily,
    avg(trip_distance_Km) as avg_trip_distance_daily,
    avg(avg_speed) as avg_speed_daily,
    sum(total_brake_events) as total_brake_events_daily, 
    sum(case when any_anomalous_event = True then 1 else 0 end) as anomalies_count_daily,
    sum(case when any_route_anomaly = True  then 1 else 0 end) as route_anomalies_daily,
    sum(case when any_geofencing_violation = True  then 1 else 0 end) as geofencing_violations_daily,
    avg(max_route_deviation) as avg_route_deviation_daily,
    avg(behavioral_consistency_index) as behavioral_consistency_index_daily
from fleet_db.analytics.trip_summary
group by driver_id, day

select * from fleet_db.analytics.driver_daily order by driver_id asc

-- create vehicle_daily table
create or replace table fleet_db.analytics.vehicle_daily as
select
    vehicle_id,
    to_date(time_stamp) as day,
    count(*) as trips_count,
    avg(trip_distance) as avg_trip_distance_daily,
    avg(fuel_consumption) as avg_fuel_consumption_daily,
    avg(rpm) as avg_rpm_daily,
    avg(speed) as avg_speed_daily
from fleet_db.clean.trips_clean
group by vehicle_id, day

select * from fleet_db.analytics.vehicle_daily order by vehicle_id asc, day asc

-- create route anomalies table
CREATE OR REPLACE TABLE fleet_db.analytics.route_anomalies AS
SELECT
  trip_id,
  driver_id,
  vehicle_id,
  to_date(time_stamp) as day,
  trip_distance,
  speed,
  (trip_duration)/60 as trip_duration_minutes,
  (trip_duration)/3600 as trip_duration_hours,
  route_anomaly,
  anomalous_event,
  geofencing_violation,
  weather_conditions,
  traffic_condition,
  road_type,
  lane_deviation,
  route_deviation_score, -- lower this values the better
  behavioral_consistency_index -- higher this value the better
FROM fleet_db.clean.trips_clean
WHERE route_anomaly = TRUE OR anomalous_event = TRUE

-- Number of trips where the route anomaly, anomalou event s and geofencing violations were True by traffic conditions 
select 
    traffic_condition,
    count(distinct trip_id) as number_of_trips
from fleet_db.analytics.route_anomalies
where route_anomaly = True and anomalous_event = True and geofencing_violation = True
group by traffic_condition

-- create daily_rollup table
CREATE OR REPLACE TABLE fleet_db.analytics.daily_rollup AS
SELECT
  to_date(time_stamp) AS day,
  COUNT(*) AS total_records,
  COUNT(DISTINCT trip_id) AS unique_trips,
  COUNT(DISTINCT driver_id) AS active_drivers,
  COUNT(DISTINCT vehicle_id) AS active_vehicles,
  AVG(speed) AS avg_speed,
  AVG(route_deviation_score) AS avg_route_deviation_score,
  sum(case when anomalous_event = True then 1 else 0 end) as anomalies_count_daily,
  sum(case when route_anomaly = True  then 1 else 0 end) as route_anomalies_daily,
  sum(case when geofencing_violation = True then 1 else 0 end) as geofencing_violations_daily
FROM fleet_db.clean.trips_clean
GROUP BY day

select * from fleet_db.analytics.daily_rollup

-- create context_summary table
create or replace table fleet_db.analytics.context_summary as
select
    to_date(time_stamp) as day,
    road_type,
    traffic_condition,
    weather_conditions,
    avg(speed) as avg_speed,
    avg(steering_angle) as avg_steering_angle,
    avg(trip_duration) as avg_trip_duration_seconds,
    avg(trip_distance) as avg_trip_distance,
    avg(fuel_consumption) as avg_fuel_consumption,
    avg(lane_deviation) as avg_lane_deviation,
    sum(stop_events) as total_stop_events,
    sum(case when anomalous_event = True then 1 else 0 end) as anomalies_count,
    sum(case when route_anomaly = True  then 1 else 0 end) as route_anomalies,
    sum(case when geofencing_violation = True then 1 else 0 end) as geofencing_violations,
    avg(route_deviation_score) as avg_route_deviation_score,
    avg(behavioral_consistency_index) as avg_driver_behaviour_score
from fleet_db.clean.trips_clean
group by day, road_type, traffic_condition, weather_conditions

select * from fleet_db.analytics.context_summary order by day asc, road_type asc;


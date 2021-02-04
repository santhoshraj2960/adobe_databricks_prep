-- Databricks notebook source
select * from global_temp.fact_fhv_taxi_trip_data
where
base_liscence_number = 'B00021';

-- COMMAND ----------

select 'green' as taxi_type,
pickup_time,
dropoff_time,
pickup_loc,
dropoff_loc,
trip_duration,
trip_type
from
global_temp.fact_green_taxi_trip_data

union

select 'yellow' as taxi_type,
pickup_time,
dropoff_time,
pickup_loc,
dropoff_loc,
trip_duration,
trip_type
from
global_temp.fact_yellow_taxi_trip_data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC taxi_zones_df = spark.read \
-- MAGIC                     .option('inferSchema', 'true') \
-- MAGIC                     .option('header', 'true') \
-- MAGIC                     .csv('/mnt/datalake/taxi_zones.csv')
-- MAGIC 
-- MAGIC taxi_zones_df.createOrReplaceTempView('taxi_zones')
-- MAGIC 
-- MAGIC '''
-- MAGIC union
-- MAGIC     select 'yellow' as taxi_type,
-- MAGIC     pickup_loc,
-- MAGIC     from
-- MAGIC     global_temp.fact_yellow_taxi_trip_data
-- MAGIC union
-- MAGIC     select base_type as taxi_type,
-- MAGIC     pickup_location as pickup_loc
-- MAGIC     from
-- MAGIC     global_temp.fact_fhv_taxi_trip_data
-- MAGIC '''

-- COMMAND ----------

select taxi_zones.zone, taxi_zones.LocationID, borough
from taxi_zones 
join
    (select 'green' as taxi_type,
    pickup_loc
    from
    global_temp.fact_green_taxi_trip_data) green_taxi_report
on
green_taxi_report.pickup_loc = taxi_zones.LocationID

-- COMMAND ----------

select pickup_location, count(*) from 
  (
  select pickup_loc from global_temp.fact_yellow_taxi_trip_data
  union all
  select pickup_loc from global_temp.fact_green_taxi_trip_data
  union all
  select pickup_loc from global_temp.fact_yellow_taxi_trip_data)

-- COMMAND ----------

select borough, taxi_type, count(*) as TotalSharedTrips
from taxi_zones 
join
    (
    select 'green' as taxi_type,
    pickup_loc
    from
    global_temp.fact_green_taxi_trip_data where trip_type = 'shared_trip'
    union all
    select 'yellow' as taxi_type,
    pickup_loc
    from
    global_temp.fact_yellow_taxi_trip_data where trip_type = 'shared_trip'
    union all
    select base_type as taxi_type,
    pickup_location as pickup_loc
    from
    global_temp.fact_fhv_taxi_trip_data where trip_type = 'shared_trip'
    ) all_taxi_report
on
all_taxi_report.pickup_loc = taxi_zones.LocationID
group by borough, taxi_type
order by borough, taxi_type


-- COMMAND ----------


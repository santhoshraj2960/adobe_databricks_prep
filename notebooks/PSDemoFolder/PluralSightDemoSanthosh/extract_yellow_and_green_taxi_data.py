# Databricks notebook source
# MAGIC %sh
# MAGIC wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2018-12.csv
# MAGIC wget -P /tmp https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2018-12.csv

# COMMAND ----------

dbutils.fs.cp("file:///tmp/yellow_tripdata_2018-12.csv", "/mnt/datalake/")
dbutils.fs.cp("file:///tmp/green_tripdata_2018-12.csv", "/mnt/datalake/")

# COMMAND ----------

# MAGIC %fs ls /mnt/datalake/

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, unix_timestamp, round, when, col

# COMMAND ----------

yellow_taxi_trip_df = spark \
    .read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/datalake/yellow_tripdata_2018-12.csv")

# COMMAND ----------

display(yellow_taxi_trip_df.describe("passenger_count", "trip_distance"))

# COMMAND ----------

filtered_yellow_taxi_trips = yellow_taxi_trip_df.where((yellow_taxi_trip_df['trip_distance'] > 0) & (yellow_taxi_trip_df['passenger_count'] > 0))

# COMMAND ----------

display(filtered_yellow_taxi_trips.describe("passenger_count", "trip_distance"))

# COMMAND ----------

print('before = ', filtered_yellow_taxi_trips.count())
filtered_yellow_taxi_trips = filtered_yellow_taxi_trips.dropna(subset=("PULocationID", "DOLocationID"))
print('after', filtered_yellow_taxi_trips.count())

# COMMAND ----------

default_values_dict = {
  "payment_type": 5,
  "RateCodeId": 1
}

filtered_yellow_taxi_trips = filtered_yellow_taxi_trips.na.fill(default_values_dict)
#filtered_yellow_taxi_trips = filtered_yellow_taxi_trips.na.fill(value=0, subset=("PULocationID"))

# COMMAND ----------

print('before = ', filtered_yellow_taxi_trips.count())
filtered_yellow_taxi_trips = filtered_yellow_taxi_trips.drop_duplicates()
print('after', filtered_yellow_taxi_trips.count())

# COMMAND ----------

print('before = ', filtered_yellow_taxi_trips.count())
filtered_yellow_taxi_trips = filtered_yellow_taxi_trips \
                              .where("tpep_pickup_datetime >= '2018-12-01' AND tpep_dropoff_datetime < '2019-01-01'")

print('after', filtered_yellow_taxi_trips.count())

# COMMAND ----------

green_taxi_trip_df = spark \
    .read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", "\t") \
    .csv("/mnt/datalake/green_tripdata_2018-12.csv")


# COMMAND ----------

display(green_taxi_trip_df.describe())

# COMMAND ----------

# MAGIC %md ###All the above cleanup oprations in a single step (Yellow taxi)

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, unix_timestamp, round, when, col

yellow_taxi_trip_df = spark \
    .read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/mnt/datalake/yellow_tripdata_2018-12.csv")

default_values_dict = {
  "payment_type": 5,
  "RateCodeId": 1
}

print('before = ', yellow_taxi_trip_df.count())

filtered_yellow_taxi_trips = yellow_taxi_trip_df.where((yellow_taxi_trip_df['trip_distance'] > 0) & (yellow_taxi_trip_df['passenger_count'] > 0)) \
                                                .dropna(subset=("PULocationID", "DOLocationID")) \
                                                .na.fill(default_values_dict) \
                                                .drop_duplicates() \
                                                .where("tpep_pickup_datetime >= '2018-12-01' AND tpep_dropoff_datetime < '2019-01-01'")

print('after', filtered_yellow_taxi_trips.count())

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ###All transformation oprations in a single step (Yellow taxi)

# COMMAND ----------

filtered_yellow_taxi_trips = filtered_yellow_taxi_trips \
                             .select( \
                              col('VendorID').alias('vendor_id'), \
                              col('tpep_pickup_datetime').alias('pickup_time'), \
                              col('tpep_dropoff_datetime').alias('dropoff_time'), \
                              col('trip_distance'), \
                              col('PULocationID').alias('pickup_loc'), \
                              col('DOLocationID').alias('dropoff_loc'), \
                              col('RatecodeID').alias('rate_code_id'), \
                              col('total_amount'), \
                              col('payment_type') \
                              ) \
                              \
                             .withColumn("trip_year", year('pickup_time')) \
                             .withColumn("trip_month", month('pickup_time')) \
                             .withColumn("trip_day", dayofmonth('pickup_time')) \
                              \
                              .withColumn("trip_duration", \
                                          round((unix_timestamp('dropoff_time') - unix_timestamp('pickup_time')) / 60) \
                                         ) \
                              \
                              .withColumn( \
                                          'trip_type', \
                                                      when(
                                                      col('rate_code_id') == 6, 'shared_trip'
                                                      )\
                                                      .when(
                                                      col('rate_code_id') == 1000, 'shared_trip' # There is no rate_code_id value of 1000. Have included for "switch case" example
                                                      )
                                                      .otherwise('solo_trip')
                                         ) \
                              .drop('rate_code_id')

# COMMAND ----------

'''
filtered_yellow_taxi_trips = filtered_yellow_taxi_trips \
                .withColumn("trip_year", year('pickup_time')) \
                .withColumn("trip_month", month('pickup_time')) \
                .withColumn("trip_day", dayofmonth('pickup_time')) \
                \
                .withColumn("trip_duration", \
                            round(unix_timestamp('pickup_time') - unix_timestamp('dropoff_time')) / 60 \
                           )

filtered_yellow_taxi_trips = filtered_yellow_taxi_trips.withColumn( \
                                                                  'trip_type', \
                                                                  when(
                                                                  col('SR_Flag') == 1, 'shared_trip'
                                                                  )\
                                                                  .when(
                                                                  col('SR_Flag') == 2, 'shared_trip'
                                                                  )
                                                                  .otherwise('solo_trip')


'''

# COMMAND ----------

# MAGIC %md ###All the above cleanup oprations in a single step (green taxi)

# COMMAND ----------

# Note the "delimiter" option. In green taxi csv, commas are not the seperators. <tab> has been used as a seperator
green_taxi_trip_df = spark \
    .read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", "\t") \
    .csv("/mnt/datalake/green_tripdata_2018-12.csv")

default_values_dict = {
  "payment_type": 5,
  "RateCodeId": 1
}

#print('before = ', green_taxi_trip_df.count())

filtered_green_taxi_trips = green_taxi_trip_df.where((green_taxi_trip_df['trip_distance'] > 0) & (green_taxi_trip_df['passenger_count'] > 0)) \
                                                .dropna(subset=("PULocationID", "DOLocationID")) \
                                                .na.fill(default_values_dict) \
                                                .drop_duplicates() \
                                                .where("lpep_pickup_datetime >= '2018-12-01' AND lpep_dropoff_datetime < '2019-01-01'")

#print('after', filtered_green_taxi_trips.count())


# COMMAND ----------

# MAGIC %md ###All transformation oprations in a single step (green taxi)

# COMMAND ----------

filtered_green_taxi_trips = filtered_green_taxi_trips \
                             .select( \
                              col('VendorID').alias('vendor_id'), \
                              col('lpep_pickup_datetime').alias('pickup_time'), \
                              col('lpep_dropoff_datetime').alias('dropoff_time'), \
                              col('trip_distance'), \
                              col('PULocationID').alias('pickup_loc'), \
                              col('DOLocationID').alias('dropoff_loc'), \
                              col('RatecodeID').alias('rate_code_id'), \
                              col('total_amount'), \
                              col('payment_type') \
                              ) \
                              \
                             .withColumn("trip_year", year('pickup_time')) \
                             .withColumn("trip_month", month('pickup_time')) \
                             .withColumn("trip_day", dayofmonth('pickup_time')) \
                              \
                              .withColumn("trip_duration", \
                                          round((unix_timestamp('dropoff_time') - unix_timestamp('pickup_time')) / 60) \
                                         ) \
                              \
                              .withColumn( \
                                          'trip_type', \
                                                      when(
                                                      col('rate_code_id') == 6, 'shared_trip'
                                                      )\
                                                      .when(
                                                      col('rate_code_id') == 1000, 'shared_trip' # There is no rate_code_id value of 1000. Have included for "switch case" example
                                                      )
                                                      .otherwise('solo_trip')
                                         ) \
                              .drop('rate_code_id')

# COMMAND ----------

filtered_yellow_taxi_trips.createOrReplaceGlobalTempView('fact_yellow_taxi_trip_data')
filtered_green_taxi_trips.createOrReplaceGlobalTempView('fact_green_taxi_trip_data')

# COMMAND ----------

display(filtered_yellow_taxi_trips)

# COMMAND ----------

display(filtered_green_taxi_trips)

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# If you partition the table based on a particular column, most likely all the rows that belong a particular "column value" will be stored as individual partitions in hdfs
# So, if there is a column that you will be querying on frequently, you should partition the table by that col

# COMMAND ----------

filtered_green_taxi_trips.write \
                              .option('header', 'true') \
                              .option('dateFormat', 'yyyy-MM-dd HH:mm:ss.S') \
                              .mode('overwrite') \
                              .csv('/mnt/datalake/dimensional_model/facts/green_taxi_fact.csv')
# other options for mode are 'append', 'overwrite', 'ignore' and 'error' https://kontext.tech/column/spark/357/save-dataframe-as-csv-file-in-spark

# COMMAND ----------

filtered_green_taxi_trips.write \
                          .option('header', 'true') \
                          .option('dateFormat', 'yyyy-MM-dd HH:mm:ss.S') \
                          .mode('ignore') \
                          .parquet('/mnt/datalake/dimensional_model/facts/green_taxi_fact.parquet')

# COMMAND ----------

green_taxi_trips_csv_df = spark.read \
                                .option('header', 'true') \
                                .csv('/mnt/datalake/dimensional_model/facts/green_taxi_fact.csv')

print(green_taxi_trips_csv_df.select('pickup_loc', 'dropoff_loc').distinct().count())

# you cannot use display() instead of print here because display can only be used to print spark dataframe objects
# since the .count() outputs a number, we used print 

# COMMAND ----------

green_taxi_trips_parquet_df = spark.read \
                                .parquet('/mnt/datalake/dimensional_model/facts/green_taxi_fact.parquet')

print(green_taxi_trips_parquet_df.select('pickup_loc', 'dropoff_loc').distinct().count())

# reading data from parquet is much faster. Writing data to parquet is slower because the schema is stored in parquet file.
# If more read operations are anticipated on a file, parquet format is better and vice versa

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create database if not exists taxi_service_warehouse;

# COMMAND ----------

green_taxi_trip_df.write \
                  .mode('overwrite') \
                  .saveAsTable("taxi_service_warehouse.green_taxi_trips_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from taxi_service_warehouse.green_taxi_trips_managed limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe taxi_service_warehouse.green_taxi_trips_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe extended taxi_service_warehouse.green_taxi_trips_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table taxi_service_warehouse.green_taxi_trips_managed;

# COMMAND ----------

green_taxi_trip_df.write \
                  .mode('overwrite') \
                  .option('path', '/mnt/datalake/dimensional_model/facts/green_taxi_fact.parquet') \
                  .saveAsTable('taxi_service_warehouse.fact_green_taxi_trips_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe extended taxi_service_warehouse.fact_green_taxi_trips_data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table taxi_service_warehouse.fact_green_taxi_trips_data

# COMMAND ----------

# MAGIC %sql
# MAGIC /* DO NOT HAVE <SPACE> BETWEEN % and SQL. Magic commands (commands that start with %) should not have space between characters */
# MAGIC CREATE TABLE IF NOT EXISTS taxi_service_warehouse.fact_green_taxi_trips_data
# MAGIC     USING parquet
# MAGIC     OPTIONS 
# MAGIC     (
# MAGIC         path "/mnt/datalake/dimensional_model/facts/green_taxi_fact.parquet"
# MAGIC     )
# MAGIC     
# MAGIC /* Another way of creating a table
# MAGIC CREATE TABLE IF NOT EXISTS taxi_service_warehouse.fact_green_taxi_trips_data
# MAGIC     USING parquet
# MAGIC     location "/mnt/datalake/dimensional_model/facts/green_taxi_fact.parquet"
# MAGIC */

# COMMAND ----------


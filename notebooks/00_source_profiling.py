# Databricks notebook source
# MAGIC %md
# MAGIC ## Source Profiling

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

source_path = (
    "/Volumes/aviation_lakehouse/landing/bts_raw"
)

flights_df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv(source_path)
)

# COMMAND ----------

row_count = flights_df.count()
column_count = len(flights_df.columns)

print(f"Rows: {row_count:,}")
print(f"Columns: {column_count}")

flights_df.printSchema()
display(flights_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check the date range

# COMMAND ----------

flights_df.select(
    F.min("FlightDate").alias("minimum_date"),
    F.max("FlightDate").alias("maximum_date")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Examine airlines and airports

# COMMAND ----------

flights_df.groupBy("Marketing_Airline_Network") \
    .count() \
    .orderBy(F.desc("count")) \
    .show()

# COMMAND ----------

flights_df.groupBy("Origin") \
    .count() \
    .orderBy(F.desc("count")) \
    .show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Examine cancellations and diversions

# COMMAND ----------

flights_df.groupBy("Cancelled", "Diverted") \
    .count() \
    .orderBy("Cancelled", "Diverted") \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test a candidate business key

# COMMAND ----------

candidate_key = [
    "FlightDate",
    "Marketing_Airline_Network",
    "Flight_Number_Marketing_Airline",
    "Origin",
    "Dest",
    "CRSDepTime"
]

duplicates_df = (
    flights_df
    .groupBy(*candidate_key)
    .count()
    .filter(F.col("count") > 1)
)

print(f"Duplicate key combinations: {duplicates_df.count():,}")
display(duplicates_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Profile nulls in important columns

# COMMAND ----------

important_columns = [
    "FlightDate",
    "Marketing_Airline_Network",
    "Flight_Number_Marketing_Airline",
    "Origin",
    "Dest",
    "CRSDepTime",
    "DepTime",
    "ArrTime",
    "DepDelay",
    "ArrDelay",
    "Cancelled",
    "Diverted"
]

null_profile = flights_df.select([
    F.sum(F.col(column).isNull().cast("int")).alias(column)
    for column in important_columns
])

display(null_profile)

# COMMAND ----------

null_pattern = (
    flights_df
    .groupBy("Cancelled", "Diverted")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.col("DepTime").isNull().cast("int"))
         .alias("null_dep_time"),
        F.sum(F.col("ArrTime").isNull().cast("int"))
         .alias("null_arr_time"),
        F.sum(F.col("DepDelay").isNull().cast("int"))
         .alias("null_dep_delay"),
        F.sum(F.col("ArrDelay").isNull().cast("int"))
         .alias("null_arr_delay")
    )
    .orderBy("Cancelled", "Diverted")
)

display(null_pattern)

# COMMAND ----------

cancelled_after_departure = (
    flights_df
    .filter(
        (F.col("Cancelled") == 1) &
        F.col("DepTime").isNotNull()
    )
    .select(
        "FlightDate",
        "Marketing_Airline_Network",
        "Flight_Number_Marketing_Airline",
        "Origin",
        "Dest",
        "CRSDepTime",
        "DepTime",
        "DepDelay",
        "CancellationCode",
        "TaxiOut",
        "WheelsOff",
        "AirTime"
    )
)

display(cancelled_after_departure.limit(30))

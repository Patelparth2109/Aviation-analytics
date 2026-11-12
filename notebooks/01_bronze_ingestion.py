# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Incremental Ingestion with Auto Loader
# MAGIC
# MAGIC Loads raw BTS CSV files from the landing volume into a Delta table as strings, adding file lineage and run metadata, then investigates duplicate business keys.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration

# COMMAND ----------

from pyspark.sql import functions as F
import uuid

source_path = (
    "/Volumes/aviation_lakehouse/landing/bts_raw/"
)

schema_path = (
    "/Volumes/aviation_lakehouse/ops/pipeline_state/"
    "bts_marketing_flights/schema"
)

checkpoint_path = (
    "/Volumes/aviation_lakehouse/ops/pipeline_state/"
    "bts_marketing_flights/checkpoint"
)

target_table = (
    "aviation_lakehouse.bronze.bts_marketing_flights"
)

ingestion_run_id = str(uuid.uuid4())

print(f"Ingestion run ID: {ingestion_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read with Auto Loader

# COMMAND ----------

raw_stream_df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("header", "true")
         .option("pathGlobFilter", "*.csv")
         .option("cloudFiles.schemaLocation", schema_path)
         .option("cloudFiles.inferColumnTypes", "false")
         .option("rescuedDataColumn", "_rescued_data")
         .load(source_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add ingestion metadata

# COMMAND ----------

bronze_stream_df = (
    raw_stream_df
    .select(
        "*",

        F.col("_metadata.file_path")
         .alias("_source_file_path"),

        F.col("_metadata.file_name")
         .alias("_source_file_name"),

        F.col("_metadata.file_size")
         .alias("_source_file_size_bytes"),

        F.col("_metadata.file_modification_time")
         .alias("_source_file_modified_at")
    )
    .withColumn(
        "_source_file_version_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("_source_file_path"),
                F.col("_source_file_size_bytes").cast("string"),
                F.col("_source_file_modified_at").cast("string")
            ),
            256
        )
    )
    .withColumn(
        "_ingested_at",
        F.current_timestamp()
    )
    .withColumn(
        "_ingestion_run_id",
        F.lit(ingestion_run_id)
    )
)

# COMMAND ----------

# Clean column names by trimming whitespace

for col_name in bronze_stream_df.columns:
    cleaned_name = col_name.strip()
    if col_name != cleaned_name:
        bronze_stream_df = bronze_stream_df.withColumnRenamed(col_name, cleaned_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write the Bronze Delta table

# COMMAND ----------

bronze_query = (
    bronze_stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(target_table)
)

bronze_query.awaitTermination()

print("Bronze ingestion completed.")

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH duplicate_keys AS (
# MAGIC     SELECT
# MAGIC         FlightDate,
# MAGIC         Marketing_Airline_Network,
# MAGIC         Flight_Number_Marketing_Airline,
# MAGIC         Origin,
# MAGIC         Dest,
# MAGIC         CRSDepTime,
# MAGIC         COUNT(*) AS records_per_key
# MAGIC     FROM aviation_lakehouse.bronze.bts_marketing_flights
# MAGIC     GROUP BY
# MAGIC         FlightDate,
# MAGIC         Marketing_Airline_Network,
# MAGIC         Flight_Number_Marketing_Airline,
# MAGIC         Origin,
# MAGIC         Dest,
# MAGIC         CRSDepTime
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS duplicate_key_groups,
# MAGIC     COALESCE(
# MAGIC         SUM(records_per_key - 1),
# MAGIC         0
# MAGIC     ) AS additional_duplicate_rows
# MAGIC FROM duplicate_keys;

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.table(
    "aviation_lakehouse.bronze.bts_marketing_flights"
)

key_columns = [
    "FlightDate",
    "Marketing_Airline_Network",
    "Flight_Number_Marketing_Airline",
    "Origin",
    "Dest",
    "CRSDepTime"
]

metadata_columns = {
    "_source_file_path",
    "_source_file_name",
    "_source_file_size_bytes",
    "_source_file_modified_at",
    "_source_file_version_id",
    "_ingested_at",
    "_ingestion_run_id",
    "_rescued_data"
}

source_columns = sorted([
    column
    for column in bronze_df.columns
    if column not in metadata_columns
])

hashed_bronze_df = bronze_df.withColumn(
    "_source_record_hash",
    F.sha2(
        F.to_json(
            F.struct(*[
                F.col(column)
                for column in source_columns
            ]),
            options={"ignoreNullFields": "false"}
        ),
        256
    )
)

duplicate_keys_df = (
    hashed_bronze_df
    .groupBy(*key_columns)
    .count()
    .filter(F.col("count") > 1)
    .drop("count")
)

duplicate_rows_df = (
    hashed_bronze_df
    .join(
        F.broadcast(duplicate_keys_df),
        on=key_columns,
        how="inner"
    )
)

# COMMAND ----------

duplicate_summary_df = (
    duplicate_rows_df
    .groupBy(*key_columns)
    .agg(
        F.count("*").alias("records"),
        F.countDistinct("_source_record_hash")
         .alias("distinct_source_payloads"),
        F.countDistinct("_source_file_path")
         .alias("distinct_source_files"),
        F.collect_set("Operating_Airline")
         .alias("operating_airlines"),
        F.collect_set("Flight_Number_Operating_Airline")
         .alias("operating_flight_numbers"),
        F.collect_set("Tail_Number")
         .alias("tail_numbers"),
        F.collect_set("Cancelled")
         .alias("cancelled_values"),
        F.collect_set("Diverted")
         .alias("diverted_values")
    )
)

display(duplicate_summary_df)

# COMMAND ----------

collected_rows = duplicate_rows_df.select(
    *source_columns,
    "_source_file_path",
    "_source_record_hash"
).collect()

grouped_rows = {}

for row in collected_rows:
    row_dict = row.asDict()

    key = tuple(
        row_dict[column]
        for column in key_columns
    )

    grouped_rows.setdefault(key, []).append(row_dict)


for key, rows in grouped_rows.items():
    print(f"\nKEY: {key}")

    if len(rows) != 2:
        print(f"Unexpected group size: {len(rows)}")
        continue

    differing_columns = [
        column
        for column in source_columns
        if rows[0][column] != rows[1][column]
    ]

    print("Differing columns:")

    for column in differing_columns:
        print(
            f"  {column}: "
            f"{rows[0][column]} | {rows[1][column]}"
        )

# COMMAND ----------

operating_key_columns = [
    "FlightDate",
    "Marketing_Airline_Network",
    "Flight_Number_Marketing_Airline",
    "Operating_Airline",
    "Flight_Number_Operating_Airline",
    "Origin",
    "Dest",
    "CRSDepTime"
]

operating_key_duplicates_df = (
    bronze_df
    .groupBy(*operating_key_columns)
    .count()
    .filter(F.col("count") > 1)
)

display(operating_key_duplicates_df)

# COMMAND ----------

display(
    bronze_df
    .filter(
        (F.col("FlightDate") == "2018-06-21") &
        (F.col("Marketing_Airline_Network") == "UA") &
        (F.col("Flight_Number_Marketing_Airline") == "3624") &
        (F.col("Operating_Airline") == "YX") &
        (F.col("Origin") == "DEN") &
        (F.col("Dest") == "XNA") &
        (F.col("CRSDepTime") == "2007")
    )
    .select(
        "FlightDate",
        "Marketing_Airline_Network",
        "Flight_Number_Marketing_Airline",
        "Operating_Airline",
        "Flight_Number_Operating_Airline",
        "Tail_Number",
        "Origin",
        "Dest",
        "CRSDepTime",
        "DepTime",
        "DepDelay",
        "WheelsOff",
        "WheelsOn",
        "CRSArrTime",
        "ArrTime",
        "ArrDelay",
        "Cancelled",
        "CancellationCode",
        "Diverted",
        "CRSElapsedTime",
        "ActualElapsedTime",
        "AirTime",
        "TaxiOut",
        "TaxiIn",
        "CarrierDelay",
        "WeatherDelay",
        "NASDelay",
        "SecurityDelay",
        "LateAircraftDelay",
        "_source_file_name"
    )
)

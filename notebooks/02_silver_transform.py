# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Typed, Validated Flight Records
# MAGIC
# MAGIC Casts Bronze string columns to typed fields, derives `flight_status` and schedule keys, and applies row-level data-quality rules. Every Bronze row lands in Silver with `dq_status` (`ACCEPTED` / `QUARANTINED`) and the list of `dq_reasons`, and the notebook fails if Bronze and Silver row counts don't reconcile.

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F

bronze_table = "aviation_lakehouse.bronze.bts_marketing_flights"
silver_table = "aviation_lakehouse.silver.flight_records"

# COMMAND ----------

def try_int(name):
    return F.expr(
        f"try_cast(try_cast(`{name}` AS DOUBLE) AS INT)"
    )

def try_double(name):
    return F.expr(f"try_cast(`{name}` AS DOUBLE)")

def clean_string(name):
    value = F.trim(F.col(name))
    return F.when(
        value.isNull() | (F.length(value) == 0),
        F.lit(None).cast("string")
    ).otherwise(value)

def is_missing(name):
    value = F.col(name)
    return value.isNull() | (
        F.trim(value.cast("string")) == ""
    )

def valid_hhmm(name):
    value = F.col(name)
    return value.isNull() | (
        (value >= 0) &
        (value <= 2400) &
        ((value % 100) < 60) &
        ((value != 2400) | ((value % 100) == 0))
    )

def key_hash(*names):
    return F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(
                    F.col(name).cast("string"),
                    F.lit("<NULL>")
                )
                for name in names
            ]
        ),
        256
    )

# COMMAND ----------

bronze_df = spark.table(bronze_table)

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

source_columns = sorted(
    column
    for column in bronze_df.columns
    if column not in metadata_columns
)

bronze_df = bronze_df.withColumn(
    "flight_record_id",
    F.sha2(
        F.to_json(
            F.struct(*[F.col(c) for c in source_columns]),
            options={"ignoreNullFields": "false"}
        ),
        256
    )
)

# COMMAND ----------

silver_base_df = bronze_df.select(
    "flight_record_id",

    F.expr("try_cast(`FlightDate` AS DATE)")
        .alias("flight_date"),
    try_int("Year").alias("source_year"),
    try_int("Quarter").alias("source_quarter"),
    try_int("Month").alias("source_month"),
    try_int("DayofMonth").alias("source_day_of_month"),
    try_int("DayOfWeek").alias("day_of_week"),

    clean_string("Marketing_Airline_Network")
        .alias("marketing_carrier_code"),
    try_int("DOT_ID_Marketing_Airline")
        .alias("marketing_carrier_dot_id"),
    try_int("Flight_Number_Marketing_Airline")
        .alias("marketing_flight_number"),

    clean_string("Operating_Airline")
        .alias("operating_carrier_code"),
    try_int("DOT_ID_Operating_Airline")
        .alias("operating_carrier_dot_id"),
    try_int("Flight_Number_Operating_Airline")
        .alias("operating_flight_number"),
    clean_string("Tail_Number").alias("tail_number"),

    try_int("OriginAirportID").alias("origin_airport_id"),
    clean_string("Origin").alias("origin_airport_code"),
    clean_string("OriginCityName").alias("origin_city_name"),
    clean_string("OriginState").alias("origin_state_code"),
    clean_string("OriginStateName").alias("origin_state_name"),

    try_int("DestAirportID").alias("destination_airport_id"),
    clean_string("Dest").alias("destination_airport_code"),
    clean_string("DestCityName").alias("destination_city_name"),
    clean_string("DestState").alias("destination_state_code"),
    clean_string("DestStateName").alias("destination_state_name"),

    try_int("CRSDepTime").alias("scheduled_departure_hhmm"),
    try_int("DepTime").alias("actual_departure_hhmm"),
    try_int("CRSArrTime").alias("scheduled_arrival_hhmm"),
    try_int("ArrTime").alias("actual_arrival_hhmm"),

    try_double("DepDelay").alias("departure_delay_minutes"),
    try_double("DepDelayMinutes")
        .alias("departure_delay_nonnegative_minutes"),
    try_int("DepDel15").alias("departure_delayed_15"),

    try_double("ArrDelay").alias("arrival_delay_minutes"),
    try_double("ArrDelayMinutes")
        .alias("arrival_delay_nonnegative_minutes"),
    try_int("ArrDel15").alias("arrival_delayed_15"),

    try_int("Cancelled").alias("cancelled"),
    clean_string("CancellationCode").alias("cancellation_code"),
    try_int("Diverted").alias("diverted"),

    try_double("CRSElapsedTime")
        .alias("scheduled_elapsed_minutes"),
    try_double("ActualElapsedTime")
        .alias("actual_elapsed_minutes"),
    try_double("AirTime").alias("air_time_minutes"),
    try_double("TaxiOut").alias("taxi_out_minutes"),
    try_double("TaxiIn").alias("taxi_in_minutes"),
    try_double("Distance").alias("distance_miles"),

    try_double("CarrierDelay").alias("carrier_delay_minutes"),
    try_double("WeatherDelay").alias("weather_delay_minutes"),
    try_double("NASDelay").alias("nas_delay_minutes"),
    try_double("SecurityDelay").alias("security_delay_minutes"),
    try_double("LateAircraftDelay")
        .alias("late_aircraft_delay_minutes"),

    "_source_file_path",
    "_source_file_name",
    "_source_file_version_id",
    "_ingested_at",
    "_ingestion_run_id",
    "_rescued_data"
)

# COMMAND ----------

silver_derived_df = (
    silver_base_df
    .withColumn(
        "flight_status",
        F.when(
            (F.col("cancelled") == 0) &
            (F.col("diverted") == 0),
            "COMPLETED"
        )
        .when(
            (F.col("cancelled") == 1) &
            (F.col("diverted") == 0),
            "CANCELLED"
        )
        .when(
            (F.col("cancelled") == 0) &
            (F.col("diverted") == 1),
            "DIVERTED"
        )
        .otherwise("UNKNOWN")
    )
    .withColumn(
        "scheduled_departure_hour",
        F.when(
            F.col("scheduled_departure_hhmm") == 2400,
            F.lit(0)
        )
        .otherwise(
            F.floor(
                F.col("scheduled_departure_hhmm") / 100
            )
        )
        .cast("int")
    )
    .withColumn(
        "marketing_schedule_key",
        key_hash(
            "flight_date",
            "marketing_carrier_code",
            "marketing_flight_number",
            "origin_airport_code",
            "destination_airport_code",
            "scheduled_departure_hhmm"
        )
    )
    .withColumn(
        "operating_schedule_key",
        key_hash(
            "flight_date",
            "marketing_carrier_code",
            "marketing_flight_number",
            "operating_carrier_code",
            "operating_flight_number",
            "origin_airport_code",
            "destination_airport_code",
            "scheduled_departure_hhmm"
        )
    )
)

missing_identity = reduce(
    lambda left, right: left | right,
    [
        is_missing(column)
        for column in [
            "flight_date",
            "marketing_carrier_code",
            "marketing_flight_number",
            "origin_airport_code",
            "destination_airport_code",
            "scheduled_departure_hhmm"
        ]
    ]
)

invalid_status = (
    F.col("cancelled").isNull() |
    F.col("diverted").isNull() |
    (~F.col("cancelled").isin(0, 1)) |
    (~F.col("diverted").isin(0, 1))
)

date_mismatch = (
    F.col("flight_date").isNotNull() &
    (
        (F.year("flight_date") != F.col("source_year")) |
        (F.month("flight_date") != F.col("source_month")) |
        (
            F.dayofmonth("flight_date") !=
            F.col("source_day_of_month")
        )
    )
)

completed_missing_operations = (
    (F.col("flight_status") == "COMPLETED") &
    (
        F.col("actual_departure_hhmm").isNull() |
        F.col("actual_arrival_hhmm").isNull() |
        F.col("departure_delay_minutes").isNull() |
        F.col("arrival_delay_minutes").isNull()
    )
)

diverted_missing_departure = (
    (F.col("flight_status") == "DIVERTED") &
    (
        F.col("actual_departure_hhmm").isNull() |
        F.col("departure_delay_minutes").isNull()
    )
)

quality_rules = [
    F.when(
        missing_identity,
        "MISSING_MARKETING_SCHEDULE_IDENTITY"
    ),
    F.when(
        is_missing("operating_carrier_code") |
        is_missing("operating_flight_number"),
        "MISSING_OPERATING_IDENTITY"
    ),
    F.when(
        F.col("flight_date").isNull(),
        "INVALID_FLIGHT_DATE"
    ),
    F.when(
        date_mismatch,
        "DATE_COMPONENT_MISMATCH"
    ),
    F.when(
        invalid_status,
        "INVALID_STATUS_INDICATOR"
    ),
    F.when(
        (F.col("cancelled") == 1) &
        (F.col("diverted") == 1),
        "CANCELLED_AND_DIVERTED"
    ),
    F.when(
        F.col("scheduled_arrival_hhmm").isNull(),
        "MISSING_SCHEDULED_ARRIVAL_TIME"
    ),
    F.when(
        ~valid_hhmm("scheduled_departure_hhmm"),
        "INVALID_SCHEDULED_DEPARTURE_TIME"
    ),
    F.when(
        ~valid_hhmm("scheduled_arrival_hhmm"),
        "INVALID_SCHEDULED_ARRIVAL_TIME"
    ),
    F.when(
        ~valid_hhmm("actual_departure_hhmm"),
        "INVALID_ACTUAL_DEPARTURE_TIME"
    ),
    F.when(
        ~valid_hhmm("actual_arrival_hhmm"),
        "INVALID_ACTUAL_ARRIVAL_TIME"
    ),
    F.when(
        completed_missing_operations,
        "COMPLETED_FLIGHT_MISSING_OPERATIONS"
    ),
    F.when(
        diverted_missing_departure,
        "DIVERTED_FLIGHT_MISSING_DEPARTURE"
    ),
    F.when(
        (F.col("flight_status") == "DIVERTED") &
        F.col("arrival_delay_minutes").isNotNull(),
        "DIVERTED_FLIGHT_HAS_ARRIVAL_DELAY"
    )
]

silver_df = (
    silver_derived_df
    .withColumn(
        "dq_reasons",
        F.filter(
            F.array(*quality_rules),
            lambda reason: reason.isNotNull()
        )
    )
    .withColumn(
        "dq_status",
        F.when(
            F.size("dq_reasons") == 0,
            "ACCEPTED"
        )
        .otherwise("QUARANTINED")
    )
    .withColumn(
        "_silver_processed_at",
        F.current_timestamp()
    )
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW
aviation_lakehouse.silver.flight_accepted AS
SELECT *
FROM {silver_table}
WHERE dq_status = 'ACCEPTED'
""")

spark.sql(f"""
CREATE OR REPLACE VIEW
aviation_lakehouse.silver.flight_quarantine AS
SELECT *
FROM {silver_table}
WHERE dq_status = 'QUARANTINED'
""")

bronze_count = spark.table(bronze_table).count()
silver_count = spark.table(silver_table).count()

if bronze_count != silver_count:
    raise ValueError(
        f"Reconciliation failed: "
        f"Bronze={bronze_count:,}, "
        f"Silver={silver_count:,}"
    )

print(
    f"Silver completed successfully: "
    f"{silver_count:,} rows"
)

display(
    spark.table(silver_table)
    .groupBy("flight_status", "dq_status")
    .count()
    .orderBy("flight_status", "dq_status")
)

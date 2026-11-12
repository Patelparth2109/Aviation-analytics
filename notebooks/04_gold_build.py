# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Star Schema for Power BI
# MAGIC
# MAGIC Builds the Gold-layer dimensional model from `aviation_lakehouse.silver.flight_accepted` for flight performance, delays, cancellations, airline reliability, and route efficiency reporting. Produces a transactional fact, a daily aggregate fact, and conformed dimensions (role-playing airline/airport copies, route, date, time, cancellation reason).

# COMMAND ----------

# MAGIC %pip install -q holidays

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "aviation_lakehouse"
SILVER_ACCEPTED = f"{CATALOG}.silver.flight_accepted"
GOLD = f"{CATALOG}.gold"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD}")
spark.sql(f"USE CATALOG {CATALOG}")

silver_df = spark.table(SILVER_ACCEPTED)
print(f"Source accepted rows: {silver_df.count():,}")

# COMMAND ----------

import holidays as hl
from datetime import date, timedelta

start_date = date(2018, 1, 1)
end_date = date(2026, 12, 31)
us_holidays = hl.US(years=range(start_date.year, end_date.year + 1))

date_rows = []
d = start_date
while d <= end_date:
    date_key = d.year * 10000 + d.month * 100 + d.day
    dow = d.isoweekday()  # 1 = Monday ... 7 = Sunday (matches BTS)
    quarter = (d.month - 1) // 3 + 1
    date_rows.append((
        date_key, d, d.year, quarter, d.month, d.strftime("%B"),
        d.day, dow, d.strftime("%A"),
        1 if dow in (6, 7) else 0,
        d.isocalendar()[1],
        1 if d in us_holidays else 0,
    ))
    d += timedelta(days=1)

date_schema = (
    "date_key int, date date, year int, quarter int, month int, "
    "month_name string, day_of_month int, day_of_week int, day_name string, "
    "is_weekend int, week_of_year int, is_us_holiday int"
)
dim_date = spark.createDataFrame(date_rows, schema=date_schema)
(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.dim_date")
)
print(f"dim_date: {dim_date.count()} rows (2018-2026)")

# COMMAND ----------

# Static carrier code -> name map (reproduces BTS L_UNIQUE_CARRIERS for carriers present)
CARRIER_NAMES = {
    "AA": "American Airlines Inc.",
    "DL": "Delta Air Lines Inc.",
    "UA": "United Air Lines Inc.",
    "WN": "Southwest Airlines Co.",
    "AS": "Alaska Airlines Inc.",
    "B6": "JetBlue Airways",
    "NK": "Spirit Air Lines",
    "F9": "Frontier Airlines Inc.",
    "G4": "Allegiant Air",
    "HA": "Hawaiian Airlines Inc.",
    "VX": "Virgin America",
    "OO": "SkyWest Airlines Inc.",
    "YX": "Republic Airways",
    "MQ": "Envoy Air Inc.",
    "OH": "PSA Airlines Inc.",
    "9E": "Endeavor Air Inc.",
    "YV": "Mesa Airlines Inc.",
    "PT": "Piedmont Airlines Inc.",
    "QX": "Horizon Air",
    "ZW": "Air Wisconsin Airlines",
    "C5": "CommutAir",
    "G7": "GoJet Airlines",
    "EV": "ExpressJet Airlines Inc.",
    "CP": "Compass Airlines",
    "AX": "Trans States Airlines",
    "EM": "Empire Airlines",
    "KS": "Peninsula Airways",
    "9K": "Cape Air",
}
MAJOR_CARRIERS = {"AA", "DL", "UA", "WN", "AS", "B6", "NK", "F9", "G4", "HA"}

carrier_map_rows = [
    (code, name, 1 if code in MAJOR_CARRIERS else 0)
    for code, name in CARRIER_NAMES.items()
]
carrier_map_df = spark.createDataFrame(
    carrier_map_rows,
    schema="carrier_code string, carrier_name string, is_major_carrier int",
)

def build_airline_dim(df, id_col, code_col):
    return (
        df.select(F.col(id_col).alias("airline_id"), F.col(code_col).alias("carrier_code"))
        .filter(F.col("airline_id").isNotNull() & F.col("carrier_code").isNotNull())
        .distinct()
        .join(carrier_map_df, "carrier_code", "left")
        .withColumn("carrier_name", F.coalesce(F.col("carrier_name"), F.col("carrier_code")))
        .withColumn("is_major_carrier", F.coalesce(F.col("is_major_carrier"), F.lit(0)))
        .select("airline_id", "carrier_code", "carrier_name", "is_major_carrier")
        .orderBy("airline_id")
    )

dim_marketing_airline = build_airline_dim(silver_df, "marketing_carrier_dot_id", "marketing_carrier_code")
dim_operating_airline = build_airline_dim(silver_df, "operating_carrier_dot_id", "operating_carrier_code")

for name, dim in [("dim_marketing_airline", dim_marketing_airline),
                  ("dim_operating_airline", dim_operating_airline)]:
    (
        dim.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD}.{name}")
    )
    print(f"{name}: {dim.count()} carriers")

# COMMAND ----------

def airport_cols(prefix):
    return [
        F.col(f"{prefix}_airport_id").alias("airport_id"),
        F.col(f"{prefix}_airport_code").alias("airport_code"),
        F.col(f"{prefix}_city_name").alias("city_name"),
        F.col(f"{prefix}_state_code").alias("state_code"),
        F.col(f"{prefix}_state_name").alias("state_name"),
    ]

origin_airports = silver_df.select(airport_cols("origin")).distinct()
destination_airports = silver_df.select(airport_cols("destination")).distinct()

# Master airport set (union of origin + destination), deduplicated by airport_id
master_airports = (
    origin_airports.unionByName(destination_airports)
    .dropDuplicates(["airport_id"])
    .filter(F.col("airport_id").isNotNull())
    .orderBy("airport_id")
)

# Physical role-playing copies (same content, separate tables for simple Power BI joins)
for name in ["dim_origin_airport", "dim_destination_airport"]:
    (
        master_airports.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD}.{name}")
    )
print(f"dim_origin_airport / dim_destination_airport: {master_airports.count()} airports")

# COMMAND ----------

dim_route = (
    silver_df.groupBy(
        "origin_airport_id", "destination_airport_id",
        "origin_airport_code", "destination_airport_code",
        "origin_city_name", "destination_city_name",
    )
    .agg(F.first("distance_miles", ignorenulls=True).alias("typical_distance_miles"))
    .withColumn(
        "route_id",
        F.sha2(F.concat_ws("||", F.col("origin_airport_id"), F.col("destination_airport_id")), 256),
    )
    .withColumn(
        "route_label",
        F.concat_ws(" \u2192 ", F.col("origin_airport_code"), F.col("destination_airport_code")),
    )
    .select(
        "route_id", "origin_airport_id", "destination_airport_id",
        "origin_airport_code", "destination_airport_code",
        "origin_city_name", "destination_city_name",
        "typical_distance_miles", "route_label",
    )
    .orderBy("route_id")
)
(
    dim_route.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.dim_route")
)
print(f"dim_route: {dim_route.count()} routes")

# COMMAND ----------

def part_of_day(h):
    if h < 5:
        return "Late Night"
    if h < 9:
        return "Early Morning"
    if h < 12:
        return "Morning"
    if h < 17:
        return "Afternoon"
    if h < 21:
        return "Evening"
    return "Night"

time_rows = [
    (h, h, f"{h:02d}:00", f"{h*100:04d}-{h*100+59:04d}", part_of_day(h))
    for h in range(24)
]
dim_time = spark.createDataFrame(
    time_rows,
    schema="time_key int, hour int, hour_label string, dep_time_block string, part_of_day string",
)
(
    dim_time.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.dim_time")
)
print(f"dim_time: {dim_time.count()} rows")

# COMMAND ----------

cancel_rows = [
    ("A", "Carrier"),
    ("B", "Weather"),
    ("C", "National Air System"),
    ("D", "Security"),
]
dim_cancellation_reason = spark.createDataFrame(
    cancel_rows, schema="cancellation_code string, cancellation_reason string"
)
(
    dim_cancellation_reason.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.dim_cancellation_reason")
)
print(f"dim_cancellation_reason: {dim_cancellation_reason.count()} rows")

# COMMAND ----------

fact_src = (
    silver_df
    .withColumn(
        "date_key",
        (F.year("flight_date") * 10000 + F.month("flight_date") * 100 + F.dayofmonth("flight_date")).cast("int"),
    )
    .withColumn(
        "route_id",
        F.sha2(F.concat_ws("||", F.col("origin_airport_id"), F.col("destination_airport_id")), 256),
    )
    .withColumn("block_time_variance", F.col("actual_elapsed_minutes") - F.col("scheduled_elapsed_minutes"))
    .withColumn("flights", F.lit(1))
    .select(
        "flight_record_id",
        "flight_date",
        "date_key",
        F.col("marketing_carrier_dot_id").alias("marketing_airline_id"),
        F.col("operating_carrier_dot_id").alias("operating_airline_id"),
        "origin_airport_id", "destination_airport_id",
        "route_id", "scheduled_departure_hour", "cancellation_code",
        "marketing_schedule_key", "operating_schedule_key",
        "tail_number", "marketing_flight_number", "operating_flight_number",
        "marketing_carrier_code", "operating_carrier_code",
        "flights", "cancelled", "diverted",
        "departure_delayed_15", "arrival_delayed_15",
        "departure_delay_minutes", "arrival_delay_minutes",
        "departure_delay_nonnegative_minutes", "arrival_delay_nonnegative_minutes",
        "carrier_delay_minutes", "weather_delay_minutes", "nas_delay_minutes",
        "security_delay_minutes", "late_aircraft_delay_minutes",
        "scheduled_elapsed_minutes", "actual_elapsed_minutes", "air_time_minutes",
        "taxi_out_minutes", "taxi_in_minutes", "distance_miles", "block_time_variance",
        "flight_status", "source_year",
    )
)

fact_src.createOrReplaceTempView("fact_flight_performance_src")

# Idempotent full refresh with liquid clustering (matches silver's full-refresh pattern).
# Switch to MERGE on flight_record_id once silver becomes incremental.
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.fact_flight_performance
USING DELTA
CLUSTER BY (flight_date, marketing_airline_id)
AS SELECT * FROM fact_flight_performance_src
""")

print(f"fact_flight_performance: {spark.table(f'{GOLD}.fact_flight_performance').count():,} rows")

# COMMAND ----------

summary_src = (
    silver_df
    .withColumn(
        "date_key",
        (F.year("flight_date") * 10000 + F.month("flight_date") * 100 + F.dayofmonth("flight_date")).cast("int"),
    )
    # Grain: one row per date x marketing airline (~32k rows, a true aggregate).
    # The finer date x airline x origin x dest x hour grain collapsed to ~1 row per
    # flight (56.6M), so it provided no aggregation benefit.
    .groupBy(
        "date_key",
        F.col("marketing_carrier_dot_id").alias("marketing_airline_id"),
    )
    .agg(
        F.count(F.lit(1)).alias("total_flights"),
        F.sum(F.when(F.col("flight_status") == "COMPLETED", 1).otherwise(0)).alias("completed_flights"),
        F.sum("cancelled").alias("cancelled_flights"),
        F.sum("diverted").alias("diverted_flights"),
        F.sum("departure_delayed_15").alias("dep_delayed_15_count"),
        F.sum("arrival_delayed_15").alias("arr_delayed_15_count"),
        F.sum(
            F.when((F.col("arrival_delayed_15") == 0) & (F.col("flight_status") == "COMPLETED"), 1).otherwise(0)
        ).alias("on_time_arrival_count"),
        F.sum("departure_delay_minutes").alias("sum_dep_delay_minutes"),
        F.sum("arrival_delay_minutes").alias("sum_arr_delay_minutes"),
        F.avg("departure_delay_minutes").alias("avg_dep_delay_minutes"),
        F.avg("arrival_delay_minutes").alias("avg_arr_delay_minutes"),
        F.sum("carrier_delay_minutes").alias("sum_carrier_delay_minutes"),
        F.sum("weather_delay_minutes").alias("sum_weather_delay_minutes"),
        F.sum("nas_delay_minutes").alias("sum_nas_delay_minutes"),
        F.sum("security_delay_minutes").alias("sum_security_delay_minutes"),
        F.sum("late_aircraft_delay_minutes").alias("sum_late_aircraft_delay_minutes"),
        F.avg("air_time_minutes").alias("avg_air_time_minutes"),
        F.avg("taxi_out_minutes").alias("avg_taxi_out_minutes"),
        F.avg("taxi_in_minutes").alias("avg_taxi_in_minutes"),
        F.avg(F.col("actual_elapsed_minutes") - F.col("scheduled_elapsed_minutes")).alias("avg_block_time_variance"),
        F.avg("distance_miles").alias("avg_distance_miles"),
    )
    .withColumn("source_year", (F.col("date_key") / 10000).cast("int"))
)

summary_src.createOrReplaceTempView("fact_flight_daily_summary_src")

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLD}.fact_flight_daily_summary
USING DELTA
PARTITIONED BY (source_year)
AS SELECT * FROM fact_flight_daily_summary_src
""")

print(f"fact_flight_daily_summary: {spark.table(f'{GOLD}.fact_flight_daily_summary').count():,} rows")

# COMMAND ----------

print("=== Gold layer row counts ===")
for t in [
    "dim_date", "dim_marketing_airline", "dim_operating_airline",
    "dim_origin_airport", "dim_destination_airport", "dim_route",
    "dim_time", "dim_cancellation_reason",
    "fact_flight_performance", "fact_flight_daily_summary",
]:
    print(f"  {t}: {spark.table(f'{GOLD}.{t}').count():,}")

fact = f"{GOLD}.fact_flight_performance"
print("\n=== Referential integrity (orphan fact rows, should be 0) ===")
orphan_checks = [
    ("date_key", f"SELECT count(*) AS c FROM {fact} f LEFT ANTI JOIN {GOLD}.dim_date d ON f.date_key = d.date_key"),
    ("marketing_airline_id", f"SELECT count(*) AS c FROM {fact} f LEFT ANTI JOIN {GOLD}.dim_marketing_airline a ON f.marketing_airline_id = a.airline_id"),
    ("operating_airline_id", f"SELECT count(*) AS c FROM {fact} f LEFT ANTI JOIN {GOLD}.dim_operating_airline a ON f.operating_airline_id = a.airline_id"),
    ("origin_airport_id", f"SELECT count(*) AS c FROM {fact} f LEFT ANTI JOIN {GOLD}.dim_origin_airport a ON f.origin_airport_id = a.airport_id"),
    ("destination_airport_id", f"SELECT count(*) AS c FROM {fact} f LEFT ANTI JOIN {GOLD}.dim_destination_airport a ON f.destination_airport_id = a.airport_id"),
    ("route_id", f"SELECT count(*) AS c FROM {fact} f LEFT ANTI JOIN {GOLD}.dim_route r ON f.route_id = r.route_id"),
    ("scheduled_departure_hour", f"SELECT count(*) AS c FROM {fact} f LEFT ANTI JOIN {GOLD}.dim_time t ON f.scheduled_departure_hour = t.time_key"),
]
for label, sql in orphan_checks:
    c = spark.sql(sql).collect()[0][0]
    print(f"  {label}: {c}")

print("\n=== Sample fact (KPI sanity) ===")
display(
    spark.sql(f"""
    SELECT
      sum(flights) AS flights,
      sum(cancelled) AS cancelled,
      round(sum(cancelled) / sum(flights) * 100, 3) AS cancel_rate_pct,
      round(sum(arrival_delayed_15) / sum(flights) * 100, 3) AS arr_delay15_pct
    FROM {fact}
    """)
)

# COMMAND ----------

import io
import requests
import pandas as pd
from pyspark.sql import functions as F

# 1) Fetch the public OurAirports dataset (IATA code -> lat/long + facility name)
URL = "https://ourairports.com/data/airports.csv"
resp = requests.get(URL, timeout=120)
resp.raise_for_status()
coords_pdf = pd.read_csv(io.BytesIO(resp.content))
coords_pdf = coords_pdf[coords_pdf["iata_code"].notna() & (coords_pdf["iso_country"] == "US")]
coords_pdf = (
    coords_pdf[["iata_code", "latitude_deg", "longitude_deg", "name", "municipality"]]
    .rename(columns={
        "iata_code": "airport_code",
        "latitude_deg": "latitude",
        "longitude_deg": "longitude",
        "name": "airport_name",
        "municipality": "facility_municipality",
    })
    .drop_duplicates(subset=["airport_code"])
)
coords_df = spark.createDataFrame(coords_pdf)
print(f"US airports with IATA coordinates available: {coords_df.count()}")

# 2) Rebuild the two role-playing airport dims with coordinates joined on IATA code
def enrich_airport_dim(table_name):
    base = spark.table(f"aviation_lakehouse.gold.{table_name}")
    enriched = (
        base.alias("b").join(coords_df.alias("c"), "airport_code", "left")
        .select(
            F.col("b.airport_id"), F.col("airport_code"),
            F.col("b.city_name"), F.col("b.state_code"), F.col("b.state_name"),
            F.col("c.airport_name"), F.col("c.facility_municipality"),
            F.col("c.latitude"), F.col("c.longitude"),
        )
        .orderBy("airport_id")
    )
    (
        enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"aviation_lakehouse.gold.{table_name}")
    )
    total = enriched.count()
    matched = enriched.filter("latitude is not null").count()
    print(f"{table_name}: {total} airports, {matched} matched with coordinates "
          f"({round(matched / total * 100, 1)}%)")
    return enriched

enrich_airport_dim("dim_origin_airport")
enrich_airport_dim("dim_destination_airport")

# 3) Coverage check: which of our airports are unmatched?
display(
    spark.sql("""
    SELECT airport_code, city_name, state_code
    FROM aviation_lakehouse.gold.dim_origin_airport
    WHERE latitude IS NULL
    ORDER BY airport_code
    """)
)

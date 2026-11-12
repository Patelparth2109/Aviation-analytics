# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Data-Quality Rule Tests
# MAGIC
# MAGIC Injects known-bad records (missing origin, invalid clock times, conflicting status flags, etc.) into a copy of a valid flight and checks that each one is quarantined with the expected reason.
# MAGIC
# MAGIC **Run in the same session after `02_silver_transform`** — it reuses `silver_derived_df` and `quality_rules` defined there.

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F

# COMMAND ----------

def apply_quality_rules(df):
    return (
        df
        .withColumn(
            "dq_reasons",
            F.filter(
                F.array(*quality_rules),
                lambda reason: reason.isNotNull()
            )
        )
        .withColumn(
            "dq_status",
            F.when(F.size("dq_reasons") == 0, "ACCEPTED")
             .otherwise("QUARANTINED")
        )
    )

# COMMAND ----------

base_record_df = (
    silver_derived_df
    .filter(F.col("flight_status") == "COMPLETED")
    .limit(1)
)

test_cases = [
    (
        base_record_df
        .withColumn("test_case", F.lit("missing_origin"))
        .withColumn(
            "origin_airport_code",
            F.lit(None).cast("string")
        )
    ),
    (
        base_record_df
        .withColumn("test_case", F.lit("invalid_departure_clock"))
        .withColumn(
            "scheduled_departure_hhmm",
            F.lit(1265)
        )
    ),
    (
        base_record_df
        .withColumn("test_case", F.lit("invalid_status_indicator"))
        .withColumn("cancelled", F.lit(7))
    ),
    (
        base_record_df
        .withColumn("test_case", F.lit("cancelled_and_diverted"))
        .withColumn("cancelled", F.lit(1))
        .withColumn("diverted", F.lit(1))
    ),
    (
        base_record_df
        .withColumn("test_case", F.lit("completed_missing_arrival"))
        .withColumn(
            "actual_arrival_hhmm",
            F.lit(None).cast("int")
        )
        .withColumn(
            "arrival_delay_minutes",
            F.lit(None).cast("double")
        )
    ),
    (
        base_record_df
        .withColumn("test_case", F.lit("diverted_with_arrival_delay"))
        .withColumn("cancelled", F.lit(0))
        .withColumn("diverted", F.lit(1))
        .withColumn("flight_status", F.lit("DIVERTED"))
        .withColumn("arrival_delay_minutes", F.lit(25.0))
    )
]

# COMMAND ----------

test_input_df = reduce(
    lambda left, right: left.unionByName(right),
    test_cases
)

test_result_df = apply_quality_rules(test_input_df)

display(
    test_result_df.select(
        "test_case",
        "dq_status",
        "dq_reasons"
    )
)

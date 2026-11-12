# Aviation Analytics Lakehouse

An end-to-end medallion lakehouse on **Databricks** that turns raw U.S. Bureau of Transportation Statistics (BTS) on-time performance files into a Power BI–ready star schema for flight performance, delay, cancellation, airline reliability and route efficiency reporting.

Built with PySpark, Delta Lake, Auto Loader and Unity Catalog.

## Architecture

```
 BTS CSV files (UC Volume)
        │  Auto Loader (availableNow), schema tracking, rescued data
        ▼
 ┌──────────────┐   string columns + file lineage + run ID
 │   BRONZE     │   aviation_lakehouse.bronze.bts_marketing_flights
 └──────┬───────┘
        │  type casting, derived status/keys, 14 data-quality rules
        ▼
 ┌──────────────┐   aviation_lakehouse.silver.flight_records
 │   SILVER     │     ├─ view: flight_accepted    (dq_status = ACCEPTED)
 └──────┬───────┘     └─ view: flight_quarantine  (dq_status = QUARANTINED)
        │  dimensional modelling (accepted rows only)
        ▼
 ┌──────────────┐   star schema: 2 facts + 8 dimensions
 │    GOLD      │   aviation_lakehouse.gold.*
 └──────┬───────┘
        ▼
     Power BI
```

## Notebooks

| Notebook | Layer | What it does |
|---|---|---|
| [`00_source_profiling`](notebooks/00_source_profiling.py) | Source | Profiles the raw files: row/column counts, date range, carriers and airports, cancellation/diversion mix, null patterns, and tests a candidate business key. |
| [`01_bronze_ingestion`](notebooks/01_bronze_ingestion.py) | Bronze | Incremental Auto Loader ingest into Delta with all columns kept as strings, `_rescued_data`, file path/size/modified-time lineage, a file version hash and an ingestion run ID. Then investigates duplicate keys by hashing full source payloads and diffing the conflicting rows. |
| [`02_silver_transform`](notebooks/02_silver_transform.py) | Silver | Safe casting (`try_cast`), snake_case renaming, derived `flight_status` (COMPLETED / CANCELLED / DIVERTED), departure hour, hashed marketing and operating schedule keys, and a deterministic `flight_record_id`. Applies the data-quality rules, writes Silver, creates accepted/quarantine views and fails if Bronze and Silver row counts don't reconcile. |
| [`03_silver_quality_tests`](notebooks/03_silver_quality_tests.py) | Silver | Unit-style tests for the DQ rules: injects deliberately broken records and checks each is quarantined with the right reason. Run in the same session after `02_silver_transform`. |
| [`04_gold_build`](notebooks/04_gold_build.py) | Gold | Builds the star schema, runs row-count and referential-integrity checks (orphan fact rows should be 0), and enriches the airport dimensions with latitude/longitude from OurAirports for map visuals. |

## Key design decisions

**Bronze stays raw.** Auto Loader runs with `inferColumnTypes = false`, so every source column lands as a string and nothing is lost to a bad cast. Unparseable records go to `_rescued_data`, and each row carries the file it came from plus a hash of the file's path, size and modified time.

**Finding the real grain.** The obvious key (date, marketing carrier, flight number, origin, destination, scheduled departure) is not unique. The Bronze notebook hashes each full source row and compares duplicate groups column by column, showing the duplicates differ on the *operating* carrier. Silver therefore carries two keys: `marketing_schedule_key` and a finer `operating_schedule_key`.

**Quarantine instead of drop.** Every Bronze row reaches Silver. Rows that fail a rule are flagged rather than deleted, so data loss is auditable and the pipeline can enforce Bronze = Silver row counts. Rules include:

- missing marketing or operating flight identity
- invalid or mismatched flight date components
- cancelled/diverted flags that aren't 0/1, or are both set
- clock times that aren't valid `HHMM` (e.g. `1265`, or `2400` with minutes)
- completed flights missing actual times or delays
- diverted flights missing departure data or carrying an arrival delay

## Gold data model

**Facts**

| Table | Grain | Notes |
|---|---|---|
| `fact_flight_performance` | One row per flight | Delay minutes by cause, taxi/air/elapsed times, block-time variance, distance, status flags. Liquid clustered on `flight_date`, `marketing_airline_id`. |
| `fact_flight_daily_summary` | Date × marketing airline | Pre-aggregated counts (flights, cancellations, diversions, 15-minute delays, on-time arrivals) and delay sums/averages. Partitioned by year. |

**Dimensions**

| Table | Description |
|---|---|
| `dim_date` | Calendar 2018–2026 with weekend and U.S. holiday flags |
| `dim_marketing_airline`, `dim_operating_airline` | Role-playing carrier dimensions with carrier names and a major-carrier flag |
| `dim_origin_airport`, `dim_destination_airport` | Role-playing airport dimensions with city/state and coordinates |
| `dim_route` | Origin → destination pairs with typical distance |
| `dim_time` | Scheduled departure hour, time block and part of day |
| `dim_cancellation_reason` | BTS cancellation codes A–D (Carrier, Weather, NAS, Security) |

Role-playing dimensions are written as separate physical tables to keep Power BI relationships simple.

## Running it

1. **Unity Catalog setup.** Create the catalog `aviation_lakehouse` with schemas `landing`, `ops`, `bronze`, `silver` and `gold`, plus the volumes:
   - `aviation_lakehouse.landing.bts_raw` for the raw CSVs
   - `aviation_lakehouse.ops.pipeline_state` for Auto Loader schema and checkpoint state
2. **Get the data.** Download monthly *Marketing Carrier On-Time Performance* CSVs from [BTS TranStats](https://www.transtats.bts.gov/) and upload them to the `bts_raw` volume.
3. **Import the notebooks.** In Databricks, go to *Workspace → Import* and import the files in `notebooks/` (they are Databricks source-format `.py` notebooks), or clone this repo with Databricks Git folders.
4. **Run in order:** `00` (optional) → `01` → `02` → `03` (optional, same session as `02`) → `04`.

`04_gold_build` installs the `holidays` package and downloads `airports.csv` from OurAirports, so the cluster needs internet access.

## Repository layout

```
aviation-analytics/
├── notebooks/
│   ├── 00_source_profiling.py
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transform.py
│   ├── 03_silver_quality_tests.py
│   └── 04_gold_build.py
├── .gitignore
└── README.md
```

## Data sources

- [BTS TranStats: Airline On-Time Performance](https://www.transtats.bts.gov/), U.S. Department of Transportation
- [OurAirports](https://ourairports.com/data/), public-domain airport coordinates

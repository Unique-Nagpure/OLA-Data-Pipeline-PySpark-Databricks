# OLA Ride Data Pipeline (PySpark + SQL on Databricks)

An end-to-end data pipeline that ingests synthetic OLA-style ride-hailing data
from multiple file formats, runs it through explicit data quality and
validation stages, transforms it with PySpark and Spark SQL, and lands it in
Delta tables — a medallion architecture (bronze → silver → gold) with quality
gates built in, rather than a straight ingest-and-transform pipeline.

## Why this project

Built as a companion to a Databricks-native lakehouse project, this one
focuses on a different skill: **explicit, auditable data quality and
validation stages** as separate pipeline steps — a pattern real data
engineering teams use and a common interview topic — using a multi-format
ingestion source (CSV + JSON + Parquet) rather than a single format.

## Architecture

```
   CSV / JSON / Parquet        BRONZE            report only        SILVER (valid + quarantine)     GOLD
   (ADLS raw container)  →  raw ingestion  →   DQ CHECK      →   VALIDATION      →   TRANSFORM   →  aggregates
                                                (measure)         (split & tag)        (join+enrich)
```

- **Bronze**: raw ingestion of all five sources, tagged with ingestion metadata, no filtering.
- **Data Quality Check**: measures null rates, duplicates, and referential integrity — logs results to `bronze.dq_results`, doesn't change any data.
- **Data Validation**: deduplicates and applies row-level business rules to trips, splitting output into `silver.trips` (valid) and `silver.trips_quarantine` (rejected, with a reason).
- **Data Transformation**: joins valid trips with all dimensions, adds derived columns (trip duration, fare per km, time-of-day, weekend flag), and builds four gold aggregates — using both the PySpark DataFrame API and Spark SQL.
- **Data Writing**: persists gold Delta tables, runs `OPTIMIZE`/`ZORDER`, and creates a BI-facing SQL view.

## Repo layout

```
ola-lakehouse-pyspark/
├── notebooks/                  # Databricks source-format (.py) notebooks
│   ├── 01_data_ingestion.py
│   ├── 02_data_quality_check.py
│   ├── 03_data_validation.py
│   ├── 04_data_transformation.py
│   └── 05_data_writing.py
├── notebooks_ipynb/             # Same 5 notebooks as Jupyter (.ipynb) files
│   ├── 01_Data_Ingestion.ipynb
│   ├── 02_Data_Quality_Check.ipynb
│   ├── 03_Data_Validation.ipynb
│   ├── 04_Data_Transformation.ipynb
│   └── 05_Data_Writing.ipynb
├── src/
│   ├── ingestion_utils.py
│   ├── quality_checks.py
│   ├── validation_rules.py
│   └── transformations.py
├── tests/
│   └── test_transformations.py
├── data/raw/                  # Synthetic sample data (CSV, JSON, Parquet)
│   ├── customers/customers.csv
│   ├── drivers/drivers.json
│   ├── vehicles/vehicles.parquet
│   ├── locations/locations.csv
│   └── trips/trips_2024_01.json, trips_2024_02.json
├── scripts/
│   └── generate_sample_data.py   # Regenerates the sample data above
├── jobs/
│   └── ola_pipeline_job.json
├── requirements.txt
└── README.md
```

## Data model

| Table | Format | Grain | Notes |
|---|---|---|---|
| `customers` | CSV | 1 row / customer | 40 sample customers |
| `drivers` | JSON | 1 row / driver | 25 sample drivers, linked to a vehicle |
| `vehicles` | Parquet | 1 row / vehicle | Vehicle type: Micro, Mini, Prime, SUV, Auto, Bike |
| `locations` | CSV | 1 row / area | Pickup/drop reference locations across 7 cities |
| `trips` | JSON (2 batch files) | 1 row / trip | Fact table — ~300 trips, includes deliberately dirty rows |

The trips data includes intentionally injected issues (missing foreign keys,
negative fares/distances, an invalid status, a drop-before-pickup timestamp,
an out-of-range rating, and a duplicate `trip_id`) so the DQ and validation
notebooks have real problems to catch — not just clean data flowing through.

## How to run on Databricks

Two equivalent sets of notebooks are provided — use whichever matches your
workflow:

- **`notebooks/`** — Databricks source-format `.py` files with widgets
  (`dbutils.widgets`) for parameterized `catalog`/`base_path` values. Import
  via Repos.
- **`notebooks_ipynb/`** — the same 5 stages as plain Jupyter `.ipynb` files,
  using `dbutils.fs.mount` against an ADLS container directly (edit the
  `configs` cell in `01_Data_Ingestion.ipynb` with your own tenant ID, client
  ID, and client secret from an app registration before running). Import via
  Workspace → Import, or drag-and-drop into a Databricks Repo.

1. **Upload sample data**: copy `data/raw/` to your ADLS container (or a
   Unity Catalog Volume), matching the folder structure above.
2. **Run notebooks in order**: `01` → `02` → `03` → `04` → `05`.
3. **Optional — orchestrate as a Job**: import `jobs/ola_pipeline_job.json`
   via Workflows → Create Job → Import from JSON, updating notebook paths and
   cluster spec.

## How to run tests / regenerate data locally

```bash
pip install -r requirements.txt
python scripts/generate_sample_data.py   # regenerates data/raw/ with a fixed seed
pytest tests/
```

- Owner: Millie
- Status: Draft (Block 1)
- Target dates: Week 1–2 of 16-week program
- Last updated: 2026-06-10

# Block 1 – Batch Healthcare Pipeline over Synthea OMOP

## Goal

Build a reproducible batch PySpark pipeline that:

- Ingests synthetic healthcare data from the Synthea OMOP dataset
  (`person`, `visit_occurrence`, `condition_occurrence`).
- Cleans and validates core patient and encounter data.
- Derives age and age bands.
- Produces partitioned, query-ready summary tables for:
  - Visits per patient per year by age band.
  - Top conditions by age band.
- Logs row counts and runtime, and fails loudly when data quality checks fail.

This will run locally for Block 1 (reading local `.csv.lzo` files),
with the design such that the data source can be switched to S3 / Glue in Block 2.

## In Scope (v1)

- **Data model**:
  - OMOP tables:
    - `person` (patient demographics).
    - `visit_occurrence` (encounters).
    - `condition_occurrence` (diagnoses).

- **Data source**:
  - For Block 1, local files under `data/raw/` (e.g. `person.csv.lzo`,
    `visit_occurrence.csv.lzo`, `condition_occurrence.csv.lzo`).
  - Paths and formats are configurable to enable S3 later.

- **Transformations**:
  - Standardize column names.
  - Parse and cast dates and numeric fields.
  - Derive:
    - Visit year (`visit_year`).
    - Age at visit / condition (approx via year difference).
    - Age bands: `0-17`, `18-34`, `35-49`, `50-64`, `65+`.

- **Aggregations**:
  - **Visits per patient per year by age band**:
    - Metrics: `patient_count`, `total_visits`, `avg_visits_per_patient`.
  - **Top conditions by age band**:
    - Metrics: `patient_count` per `(age_band, condition_concept_id)`,
      plus rank within each age band.

- **Validation**:
  - Null checks for key fields (IDs, dates).
  - Range checks on age (0–115).
  - Type checks on dates and numeric fields.

- **Outputs**:
  - Summary tables written under `data/processed/`, partitioned by
    `year` and/or `age_band`.
  - Validation report written when checks fail.

- **Observability**:
  - Logging of start/end time, row counts, validation failures, runtime.

## Out of Scope (Block 1)

- Writing outputs back to S3 (Block 1 may read from local or S3, but
  writing to S3 is optional and can be deferred).
- AWS Glue jobs or other managed Spark services (Block 2).
- Full OMOP vocabulary joins (concept tables).
- FHIR resources, DICOM images, or text notes.
- Any UI or dashboard; Block 1 is pipeline-only.

## Behaviour

From a single command (for example `python -m src.pipeline`), the system:

1. Loads configuration (input paths, output paths, age band definitions).
2. Creates a SparkSession.
3. Reads `person`, `visit_occurrence`, and `condition_occurrence`
   from `data/raw/` as CSV (compressed `.lzo`).
4. Cleans and standardizes the data:
   - Normalizes column names.
   - Casts types.
   - Filters out clearly invalid rows where appropriate.
5. Derives:
   - `visit_year` from `visit_start_date`.
   - Age at visit and condition.
   - `age_band` buckets.
6. Computes:
   - Summary table A: visits per patient per year by age band.
   - Summary table B: top conditions by age band.
7. Runs validation checks on:
   - Required IDs (e.g. `person_id`, `visit_occurrence_id`,
     `condition_occurrence_id`).
   - Required dates (`visit_start_date`, `condition_start_date`).
   - Age range constraints.
8. If any validation rule fails above a configurable threshold:
   - Writes a `validation_report.json` to `data/processed/`.
   - Logs the failure and exits with a non-zero status code.
9. If validations pass:
   - Writes summary tables to `data/processed/` (partitioned).
   - Logs metrics (row counts, validation stats, runtime).
   - Exits with code 0.

## Acceptance Criteria (Done = all true)

1. A single command (`python -m src.pipeline` or equivalent) runs
   the full pipeline end-to-end on my machine.

2. The pipeline reads three input tables from `data/raw/`:
   - `person*` file (Synthea OMOP `person`).
   - `visit_occurrence*` file.
   - `condition_occurrence*` file.

3. The pipeline produces a summary table with at least these columns:
   - `year` (visit year),
   - `age_band`,
   - `patient_count`,
   - `total_visits`,
   - `avg_visits_per_patient`.

4. The pipeline produces a second summary table with at least:
   - `age_band`,
   - `condition_concept_id`,
   - `patient_count`,
   - `rank_in_age_band`.

5. Age is derived from OMOP `year_of_birth` and event year, and
   bucketed into the defined age bands. Ages outside [0, 115] are
   either dropped or flagged as validation failures.

6. Validation rules are implemented and enforced:
   - Null checks on key IDs and dates.
   - Range checks on age.
   - Type checks for dates and numeric fields.

7. On validation failure, the pipeline:
   - Exits with a non-zero status code.
   - Writes a machine-readable validation report
     (e.g. `data/processed/validation_report.json`).

8. On success, the pipeline:
   - Writes both summary tables to `data/processed/` using a
     partitioning scheme that includes `year` and/or `age_band`.
   - Logs start/end time, input row counts per table, output row counts,
     number of validation failures, and total runtime.

9. The repo contains:
   - This spec (`docs/spec.md`),
   - A technical plan (`docs/plan.md`),
   - A task breakdown (`docs/tasks.md`),
   - At least one test file in `tests/` that runs successfully
     (e.g. validation tests and a smoke test for the pipeline).
- Related spec: docs/spec.md
- Scope: Block 1 (local batch pipeline)
- Future work: Block 2 (S3/Glue), Block 3+ (RAG, agents)

# Block 1 – Technical Plan

## Overview

This project will implement a local PySpark batch pipeline over the
Synthea OMOP dataset. The initial version will read local raw files,
clean and validate core tables, derive age-based features, compute
summary analytics, and write processed outputs to disk.

The design should make it easy to switch the data source from local
files to S3 / AWS Glue in Block 2 without rewriting the full pipeline.

## Objectives

- Build a reproducible batch pipeline using PySpark.
- Practice spec-driven development: spec -> plan -> tasks -> implementation.
- Separate configuration, validation, transformation, and output logic.
- Add basic tests for validation logic and pipeline smoke behavior.
- Keep the code modular enough to support cloud migration later.

## Input Data

The pipeline is expected to work with these OMOP tables:

- `person`
- `visit_occurrence`
- `condition_occurrence`

For Block 1, input files will live under `data/raw/` and may be:

- `person.csv.lzo`
- `visit_occurrence.csv.lzo`
- `condition_occurrence.csv.lzo`

If only one file is available initially, development can begin with that
table and stubbed interfaces for the other two.

## Output Data

The pipeline will write outputs under `data/processed/`.

Planned outputs:

- `visits_by_age_band/`
- `top_conditions_by_age_band/`
- `validation_report.json`
- optional log file under `logs/`

Output format may begin as CSV or Parquet depending on what is easiest
to support locally in PySpark.

## Architecture

The project will use a small modular structure:

```text
src/
  config.py
  pipeline.py
  io.py
  transform.py
  validation.py
  logging_utils.py
tests/
  test_validation.py
  test_transform.py
  test_pipeline_smoke.py
docs/
  spec.md
  plan.md
  tasks.md
data/
  raw/
  processed/
logs/
```

### Module responsibilities

#### `src/config.py`
Contains runtime configuration such as:

- input paths
- output paths
- file names
- age band definitions
- validation thresholds
- run mode flags (local now, S3 later)

This file should centralize settings so the rest of the code does not
hardcode paths.

#### `src/io.py`
Responsible for reading and writing data.

Planned functions:

- `create_spark_session()`
- `read_person(spark, path)`
- `read_visit_occurrence(spark, path)`
- `read_condition_occurrence(spark, path)`
- `write_dataframe(df, path, format, partition_cols=None)`

This module isolates data access from business logic.

#### `src/transform.py`
Responsible for cleaning and feature engineering.

Planned functions:

- `normalize_columns(df)`
- `cast_person_types(df)`
- `cast_visit_types(df)`
- `cast_condition_types(df)`
- `derive_age_band_from_year(year_col, birth_year_col)`
- `build_visits_by_age_band(person_df, visit_df)`
- `build_top_conditions_by_age_band(person_df, condition_df)`

This module should contain the main transformation logic and keep it
testable.

#### `src/validation.py`
Responsible for data quality checks.

Planned functions:

- `check_required_columns(df, expected_columns)`
- `check_non_null(df, columns)`
- `check_age_range(df, age_column, min_age=0, max_age=115)`
- `build_validation_report(results)`
- `has_blocking_failures(results, threshold_config)`

Validation output should be structured so it can be logged and written
to a JSON report.

#### `src/logging_utils.py`
Responsible for logging setup and helper methods.

Planned functions:

- `configure_logging()`
- `log_row_count(name, df)`
- `log_validation_summary(results)`
- `log_runtime(start_time, end_time)`

#### `src/pipeline.py`
The orchestration entrypoint.

Planned flow:

1. Load config.
2. Configure logging.
3. Create SparkSession.
4. Read input tables.
5. Normalize and cast data.
6. Derive features.
7. Run validation checks.
8. Stop if blocking failures occur.
9. Compute summary outputs.
10. Write outputs.
11. Log completion metrics.

This file should be thin and readable; most logic should live in
supporting modules.

## Execution Flow

The intended command is:

```bash
python -m src.pipeline
```

Expected runtime sequence:

1. Start pipeline.
2. Load config and initialize Spark.
3. Read raw OMOP tables from `data/raw/`.
4. Apply cleaning and casting.
5. Derive age-related fields and age bands.
6. Run validation.
7. If validation fails:
   - write `validation_report.json`
   - log failure
   - exit non-zero
8. If validation passes:
   - compute summary tables
   - write processed outputs
   - log success and runtime

## Transformation Plan

### Person table
Use `person` to provide demographic fields and birth year.

Planned steps:

- normalize column names
- cast `person_id` and concept IDs to numeric types where possible
- cast `year_of_birth` to integer
- retain only fields needed for Block 1

### Visit occurrence table
Use `visit_occurrence` to compute utilization.

Planned steps:

- parse `visit_start_date`
- derive `visit_year`
- join with `person` on `person_id`
- compute age at visit using `visit_year - year_of_birth`
- map age to age band
- aggregate visits by patient, year, and age band
- aggregate again to summary metrics

### Condition occurrence table
Use `condition_occurrence` to compute top conditions by age band.

Planned steps:

- parse `condition_start_date`
- derive event year if needed
- join with `person` on `person_id`
- compute age band
- aggregate distinct patients by `(age_band, condition_concept_id)`
- rank within each age band

## Validation Plan

Validation rules for v1:

- required table-level columns must exist
- key identifiers must not be null:
  - `person_id`
  - `visit_occurrence_id`
  - `condition_occurrence_id`
- required dates must be parseable and non-null:
  - `visit_start_date`
  - `condition_start_date`
- derived age must be within 0 to 115
- row counts should be logged before and after cleaning

Validation report shape:

- rule name
- table name
- failed row count
- failure percentage
- blocking vs non-blocking
- notes / sample message

## Testing Plan

Testing for Block 1 will be intentionally lightweight but real.

### Unit tests
`tests/test_validation.py`

- null-check logic
- age-range check
- required-column detection

`tests/test_transform.py`

- age-band mapping
- basic transformation logic on tiny in-memory DataFrames

### Smoke test
`tests/test_pipeline_smoke.py`

- create minimal sample data
- run the pipeline or major pipeline functions
- verify outputs are created and not empty

The goal is not perfect coverage; the goal is to practice real software
engineering habits and make refactoring safer.

## Logging and Observability

Logging should include:

- pipeline start and end
- input row counts
- output row counts
- validation failures
- total runtime
- paths used for input and output

If practical, logs should go both to console and to a file under `logs/`.

## Migration Path to Block 2

This Block 1 design should support a later move to AWS by minimizing
hardcoded local assumptions.

Planned migration-friendly choices:

- all file paths live in `config.py`
- I/O functions are isolated in `io.py`
- transformation logic does not depend on local file system details
- outputs use formats that Glue/Spark can also read later

In Block 2, likely changes will be:

- replace local paths with S3 URIs
- add Glue-compatible execution
- add cloud-oriented logging and partition strategy

## Risks and Simplifications

### Risks
- local Spark + Windows environment issues
- compressed `.lzo` handling may require extra setup
- incomplete local sample files early in development

### Simplifications
- begin with one table if needed and scaffold the others
- use documented OMOP schema assumptions before full inspection
- use simple age approximation from event year minus birth year
- defer vocabulary joins and advanced healthcare semantics

## Definition of Done

This plan is complete when:

- the repo structure exists
- all planned modules exist, even if partially stubbed at first
- a single command runs the pipeline
- validation and aggregation are implemented
- outputs are written successfully
- at least basic tests pass
- docs (`spec.md`, `plan.md`, `tasks.md`, `README.md`) are present
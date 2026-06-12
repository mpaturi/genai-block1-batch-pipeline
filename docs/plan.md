# Block 1 Plan

## Objective

Implement the initial local version of a Python/PySpark batch pipeline over a synthetic OMOP-style healthcare dataset.

This block is about creating a strong foundation:
- project structure
- reproducible setup
- synthetic data generation
- validation and transformation modules
- a runnable batch entry point
- test coverage for basic rules

## Architecture overview

The Block 1 flow will be:

1. Generate source healthcare data using Synthea and transform selected outputs into simplified OMOP-style tables.
2. Save those tables to `data/raw/`.
3. Read raw tables into PySpark using explicit schemas.
4. Validate key constraints and basic business rules.
5. Clean and standardize datatypes and date fields.
6. Build a person-level analytic dataset.
7. Write the final dataset to `data/processed/`.
8. Demonstrate the output in a Jupyter notebook.

## Planned modules

### `src/config.py`
Holds project paths, file names, row-count settings, and simple configuration constants.
REFERENCE_DATE = 2025-01-01
RANDOM_SEED = 42
These values ensure deterministic and reproducible pipeline runs.

### `src/schemas.py`
Defines the Spark schemas for:
- PERSON
- VISIT_OCCURRENCE
- CONDITION_OCCURRENCE
- DRUG_EXPOSURE
- MEASUREMENT
- NOTE


### `src/generator.py`
Contains synthetic data generation logic for all Block 1 tables.

Responsibilities:
- ingest Synthea outputs
- transform selected Synthea tables into simplified OMOP-style tables
- map Synthea records to PERSON, VISIT_OCCURRENCE,
  CONDITION_OCCURRENCE, DRUG_EXPOSURE, MEASUREMENT and NOTE
- preserve referential integrity during transformation
- maintain deterministic processing using a fixed seed and reference date

### `src/io_utils.py`
Handles reading and writing local CSV or Parquet files.

Responsibilities:
- write generated raw files
- read raw files into Spark DataFrames
- write processed outputs

### `src/validations.py`
Contains validation checks and helper functions.

Initial checks:
- event `person_id` values exist in PERSON
- optional `visit_occurrence_id` values exist in VISIT_OCCURRENCE
- start/end dates are logically ordered
- required columns are non-null where expected

### `src/transforms.py`
Contains transformation and aggregation logic.

Initial responsibilities:
- clean nulls and cast types
- standardize date fields
- compute visit counts by person
- derive chronic condition flags
- compute latest selected measurement values
- write analytic_person as partitioned Parquet output

### `src/pipeline.py`
Coordinates the full pipeline.

Responsibilities:
- read source data
- run validation steps
- run transformations
- write final output

### `src/main.py`
Command-line entry point that executes the entire Block 1 workflow from a single command, including data generation, validation, transformation, and output creation.

## Data design plan

The synthetic dataset will intentionally model a small subset of OMOP’s patient-centric structure, where event tables reference the PERSON table through `person_id`.

Design priorities for Block 1:
- clear relationships
- deterministic generation
- realistic-enough distributions
- simple concept sets
- small enough to run locally but large enough to feel realistic

Initial generation approach:
- create PERSON first
- create VISIT_OCCURRENCE next
- create CONDITION_OCCURRENCE, DRUG_EXPOSURE, MEASUREMENT and NOTE from persons and visits
- preserve referential integrity during generation rather than trying to repair it later

## Testing plan

Testing will use `pytest`, with reusable fixtures defined in `conftest.py`, which is the standard pytest mechanism for sharing fixtures across tests.

Testing layers:
1. Pure Python unit tests for helper logic.
2. Small Spark DataFrame tests for validations and transformations.
3. One smoke test for end-to-end pipeline execution on very small sample data.

PySpark testing guidance from Apache Spark supports creating reusable Spark test sessions and validating DataFrame-level logic in tests. 

## Notebook plan

`notebooks/block1_demo.ipynb` will:
- explain the project briefly
- load the analytic output
- show row counts and schema
- run simple analysis such as visit counts by age band
- show at least one simple plot

The notebook is for demonstration, not for production logic. Core pipeline code should remain in `src/`.

## Block boundaries

### Included in Block 1
- local synthetic OMOP-style data generation
- local raw and processed storage
- basic PySpark validation and transformation logic
- one person-level analytic dataset
- write analytic_person as partitioned Parquet - year_of_birth_band
- initial tests
- notebook demo
- concise documentation

### Deferred to later blocks
- more OMOP tables
- larger data volume
- Spark performance tuning
- orchestration and scheduling
- cloud storage and production deployment
- vocabulary mapping and richer healthcare semantics
- data quality dashboards and monitoring

## Completion criteria

This plan is complete when:
- the repository structure exists
- generator code creates all Block 1 raw datasets
- a single pipeline command produces the final analytic_person dataset
- key validations are implemented
- initial tests pass
- docs and README reflect the actual implementation
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

0. Run the Synthea CLI (Java) locally to generate a raw patient population into `data/synthea_raw/`.
1. Map selected Synthea CSV outputs into the simplified OMOP-style tables using `src/concepts.py` concept dictionaries, generate template-based NOTE text from visit data, inject a small share of intentionally dirty rows, and write the result to `data/raw/`.
2. Read raw tables into PySpark using explicit schemas.
3. Run validation checks on raw data to detect dirty rows (null/datatype/range/referential-integrity/date-order).
4. Clean (drop/quarantine) the detected rows, logging before/after row counts; standardize datatypes and date fields.
5. Re-run validation on the cleaned data as a hard gate — abort if issues remain.
6. Build a person-level analytic dataset.
7. Write the final dataset to `data/processed/` as partitioned Parquet.
8. Demonstrate the output in a Jupyter notebook.

### Prerequisites

Running step 0 requires a local Java installation and the Synthea release jar (`synthea-with-dependencies.jar`). `scripts/run_synthea.ps1` wraps the invocation with the project's population size and seed.

## Planned modules

### `src/config.py`
Holds project paths, file names, row-count settings, and simple configuration constants.
REFERENCE_DATE = 2025-01-01
RANDOM_SEED = 42
NUM_PERSONS = 10,000 (target PERSON row count)
TOTAL_ROW_BUDGET ≈ 100,000 (sanity check across all six tables)
DIRTY_DATA_FRACTION ≈ 0.01-0.02 per injected-issue category
Paths: `data/synthea_raw/`, `data/raw/`, `data/processed/`, `data/sample/`
These values ensure deterministic and reproducible pipeline runs.

### `src/schemas.py`
Defines the Spark schemas for:
- PERSON
- VISIT_OCCURRENCE
- CONDITION_OCCURRENCE
- DRUG_EXPOSURE
- MEASUREMENT
- NOTE
- analytic_person

### `src/concepts.py`
Defines synthetic `*_concept_id` lookup dictionaries mapping Synthea's native codes (SNOMED/RxNorm/LOINC, and gender/race/ethnicity strings) to small synthetic integers, for the concept families named in `docs/spec.md` (gender, race/ethnicity, visit types, chronic conditions + related drugs, measurements + units). Used by `src/generator.py` during mapping; codes outside the whitelist are excluded.

### `src/generator.py`
Contains synthetic data generation logic for all Block 1 tables.

Responsibilities:
- ingest Synthea CSV exports from `data/synthea_raw/`
- map Synthea records to PERSON, VISIT_OCCURRENCE, CONDITION_OCCURRENCE, DRUG_EXPOSURE, and MEASUREMENT using `src/concepts.py` lookups
- generate NOTE records with template-based `note_text` derived from visit data (visit-type-specific complaint and assessment phrases)
- inject a small, deterministic share of dirty rows (duplicates, required-field nulls, bad date pairs, out-of-range values, orphaned FK references) per `docs/spec.md`'s "Intentional data quality issues"
- write the resulting tables to `data/raw/`
- maintain deterministic processing using `RANDOM_SEED` and `REFERENCE_DATE`

### `src/io_utils.py`
Handles reading and writing local CSV or Parquet files.

Responsibilities:
- write generated raw files
- read raw files into Spark DataFrames
- write processed outputs

### `src/validations.py`
Contains validation **detection** functions — pure checks that return per-category violation counts/row identifiers without halting the pipeline:
- event `person_id` values exist in PERSON
- optional `visit_occurrence_id` values exist in VISIT_OCCURRENCE
- start/end dates are logically ordered
- required columns are non-null where expected
- numeric values fall within plausible ranges
- duplicate primary keys

Run twice by `src/pipeline.py`: once on raw data (results feed cleaning and the "validation failures" metric) and once on cleaned data (results gate output creation).

### `src/transforms.py`
Contains cleaning, transformation, and aggregation logic.

Initial responsibilities:
- drop/quarantine rows flagged by `src/validations.py`'s raw-data pass, logging before/after row counts
- cast types and standardize date fields
- compute visit counts by person
- derive chronic condition flags
- compute latest selected measurement values
- build and write analytic_person as partitioned Parquet output

### `src/pipeline.py`
Coordinates the full pipeline and serves as the command-line entry point (`python -m src.pipeline`).

Responsibilities:
- read source data
- run validation (detection) on raw data
- run cleaning and transformations
- run validation (gate) on cleaned data; abort before writing output if it fails
- write final output and pipeline metrics

## Data design plan

The synthetic dataset will intentionally model a small subset of OMOP’s patient-centric structure, where event tables reference the PERSON table through `person_id`.

Design priorities for Block 1:
- clear relationships
- deterministic generation
- realistic-enough distributions
- simple concept sets
- ~10,000 persons, ~100,000 total rows across all six tables
- a small, known share of intentionally dirty rows to exercise validation/cleaning

Initial generation approach:
- create PERSON first
- create VISIT_OCCURRENCE next
- create CONDITION_OCCURRENCE, DRUG_EXPOSURE, MEASUREMENT and NOTE from persons and visits
- preserve referential integrity during generation rather than trying to repair it later
- apply dirty-data injection as a final pass over the generated tables

## Testing plan

Testing will use `pytest`, with reusable fixtures defined in `conftest.py`, which is the standard pytest mechanism for sharing fixtures across tests.

Testing layers:
1. Pure Python unit tests for helper logic.
2. Small Spark DataFrame tests for validations and transformations.
3. One smoke test for end-to-end pipeline execution on very small sample data.

PySpark testing guidance from Apache Spark supports creating reusable Spark test sessions and validating DataFrame-level logic in tests. 

## Notebook plan

`notebooks/demo.ipynb` will:
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
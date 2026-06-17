# Block 1 Tasks

## Documentation

- [x] Finalize Block 1 scope and success criteria in `docs/spec.md`
- [x] Document selected OMOP-style tables and simplified schemas
- [x] Document relationships centered on `PERSON.person_id`
- [x] Document storage plan for `data/raw/` and `data/processed/`
- [x] Document synthetic data assumptions and initial concept sets
- [x] Define the `analytic_person` output schema and column derivations in `docs/spec.md`
- [x] Write implementation approach in `docs/plan.md`
- [x] Clarify what Block 1 includes versus what is deferred
- [x] Keep `README.md` aligned with actual current scope

## Repo and environment

- [x] Confirm top-level folders exist: `docs/`, `src/`, `tests/`, `notebooks/`, `data/`, `scripts/`
- [x] Confirm local virtual environment `myenv` works
- [x] Install Block 1 dependencies
- [x] Capture pinned dependencies in `requirements.txt`
- [x] Finalize `.gitignore` for envs, data, caches, logs, and notebook artifacts

## Schema and generation design

- [x] Finalize exact column lists for PERSON
- [x] Finalize exact column lists for VISIT_OCCURRENCE
- [x] Finalize exact column lists for CONDITION_OCCURRENCE
- [x] Finalize exact column lists for DRUG_EXPOSURE
- [x] Finalize exact column lists for MEASUREMENT
- [x] Finalize exact column lists for NOTE
- [x] Define datatypes for all columns
- [x] Define concept ID sets for visit types, conditions, drugs, and measurements
- [x] Define generation rules for demographics
- [x] Define generation rules for visit frequency
- [x] Define generation rules for chronic conditions
- [x] Define generation rules for drug exposures
- [x] Define generation rules for measurements and realistic ranges
- [x] Finalize Parquet as the processed output format

## Data generation

- [x] Implement PERSON generator
- [x] Implement VISIT_OCCURRENCE generator
- [x] Implement CONDITION_OCCURRENCE generator
- [x] Implement DRUG_EXPOSURE generator
- [x] Implement MEASUREMENT generator
- [x] Implement NOTE generator
- [x] Add deterministic seed support
- [x] Write generated tables to `data/raw/`
- [x] Verify row counts and key uniqueness

## Pipeline

- [x] Implement raw-data readers in PySpark
- [x] Add explicit schemas
- [x] Implement type casting and date parsing
- [x] Add basic null handling
- [x] Add referential integrity validation on `person_id`
- [x] Add optional referential validation on `visit_occurrence_id`
- [x] Add date-order validation
- [x] Implement visit aggregations by person
- [x] Implement condition-based flags
- [x] Implement latest-measurement logic
- [x] Generate pipeline quality report
- [x] Build final `analytic_person` dataset
- [x] Write final output to `data/processed/`

## Testing

- [x] Add `tests/conftest.py` Spark fixture
- [x] Add unit tests for helper functions
- [x] Add validation tests for `person_id` integrity
- [x] Add validation tests for date consistency
- [x] Add tests for required non-null fields
- [x] Add transformation tests for person-level aggregations
- [x] Add smoke test for pipeline run on tiny sample data

## Notebook demo

- [x] Create `notebooks/demo.ipynb`
- [x] Add short markdown intro about project and dataset
- [x] Load processed analytic output
- [x] Show row counts and schema
- [x] Add one or two simple analyses
- [x] Add one or two simple plots

## Polish

- [x] Review `.gitignore` one more time
- [x] Review README for accuracy
- [x] Make sure notebook output is present only if intentional
- [x] Make sure only tiny sample data, if any, is committed
- [x] Make sure docs reflect final Block 1 implementation

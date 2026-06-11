# Block 1 – Task Breakdown (≈10 days)

## Day 1 – Repo and docs

- [ ] Create project folder and base structure:
  - `docs/`, `src/`, `tests/`, `data/raw/`, `data/processed/`, `logs/`, `scripts/`
- [ ] Add `docs/spec.md` (from design session)
- [ ] Add `docs/plan.md` (technical plan)
- [ ] Add minimal `README.md` explaining Block 1 goal
- [ ] Move `person.csv.lzo` into `data/raw/`

## Day 2 – Basic code skeleton

- [ ] Create empty modules:
  - `src/config.py`
  - `src/io.py`
  - `src/transform.py`
  - `src/validation.py`
  - `src/logging_utils.py`
  - `src/pipeline.py`
- [ ] In `config.py`, define:
  - local input paths (e.g. `DATA_RAW_DIR`, `PERSON_PATH`, etc.)
  - output dir (`DATA_PROCESSED_DIR`)
  - age band definitions (list of tuples)
- [ ] In `pipeline.py`, stub `run_pipeline()` with `pass`
- [ ] In `scripts/`, create `run_pipeline.sh` or a note with the command to run

## Day 3 – Spark session and I/O scaffolding

- [ ] Implement `create_spark_session()` in `io.py`
  - local Spark only, minimal config
- [ ] Implement `read_person(spark, path)` in `io.py` with placeholder logic
  - for now, use `spark.read.csv` with header and inferSchema
- [ ] In `pipeline.py`, call:
  - `create_spark_session()`
  - `read_person(...)`
  - print schema or row count
- [ ] Verify the script runs end-to-end without crashing (even if it only reads `person`)

## Day 4 – Logging and simple validation utilities

- [ ] Implement `configure_logging()` in `logging_utils.py`
  - log to console; file under `logs/` optional
- [ ] Call `configure_logging()` at the top of `run_pipeline()`
- [ ] In `validation.py`, implement small utilities:
  - `check_required_columns(df, expected_columns)`
  - `check_non_null(df, columns)`
- [ ] Add `tests/test_validation.py` with simple tests for these functions
- [ ] Run tests with `pytest` and fix any issues

## Day 5 – Person table cleaning and age bands

- [ ] In `transform.py`, implement:
  - `normalize_columns(df)` for basic column-name cleanup
  - `cast_person_types(df)` (cast `person_id`, `year_of_birth`, etc.)
- [ ] Implement `derive_age_band_from_year(year_col, birth_year_col)` logic
  - age = event_year - year_of_birth
  - map to bands: `0-17`, `18-34`, `35-49`, `50-64`, `65+`
- [ ] Add `tests/test_transform.py` covering age band mapping on a tiny DataFrame

## Day 6 – Visit table and visits-by-age-band aggregation

- [ ] Implement `read_visit_occurrence(spark, path)` in `io.py`
- [ ] In `transform.py`, implement:
  - `cast_visit_types(df)` (IDs, dates)
  - logic to derive `visit_year`
- [ ] Implement `build_visits_by_age_band(person_df, visit_df)`:
  - join on `person_id`
  - compute age at visit and age band
  - aggregate to:
    - `year`, `age_band`, `patient_count`, `total_visits`, `avg_visits_per_patient`
- [ ] Add tests (even small ones) for `build_visits_by_age_band` using tiny synthetic DataFrames

## Day 7 – Condition table and top-conditions aggregation

- [ ] Implement `read_condition_occurrence(spark, path)` in `io.py`
- [ ] In `transform.py`, implement:
  - `cast_condition_types(df)` (IDs, dates)
- [ ] Implement `build_top_conditions_by_age_band(person_df, condition_df)`:
  - join on `person_id`
  - compute age band at condition time
  - aggregate distinct patients by `(age_band, condition_concept_id)`
  - compute rank per age band (e.g. using window functions)
- [ ] Extend `tests/test_transform.py` with a small test for this function

## Day 8 – Full validation and reporting

- [ ] In `validation.py`, add:
  - `check_age_range(df, age_column, min_age=0, max_age=115)`
  - a function to combine rule results into a `validation_report` structure
- [ ] In `pipeline.py`, wire validation:
  - run checks after transformations
  - collect results
  - if blocking failures, write `validation_report.json` and exit non-zero
- [ ] Add tests in `test_validation.py` for age-range and report combining

## Day 9 – Outputs, logging, and smoke test

- [ ] Implement `write_dataframe(df, path, format, partition_cols=None)` in `io.py`
- [ ] In `pipeline.py`, wire:
  - writing `visits_by_age_band` output
  - writing `top_conditions_by_age_band` output
- [ ] Add logging calls:
  - input row counts
  - output row counts
  - validation failures
  - runtime
- [ ] Add `tests/test_pipeline_smoke.py`:
  - use tiny in-memory DataFrames or sample files
  - run a simplified pipeline path and assert outputs exist

## Day 10 – Review, refactor, stretch goals

- [ ] Run the full pipeline on the real sample data
- [ ] Review code as if it were a PR:
  - naming, module structure, comments, docstrings
- [ ] Update `docs/spec.md` and `docs/plan.md` if the implementation diverged
- [ ] Expand `README.md`:
  - brief description
  - how to set up environment
  - how to run pipeline
- [ ] Optional stretch:
  - add config option to read from S3 instead of local
  - `aws s3 cp` additional OMOP files and point config at the bucket
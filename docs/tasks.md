# Block 1 Tasks

## Documentation

- [ ] Finalize Block 1 scope and success criteria in `docs/spec.md`
- [ ] Document selected OMOP-style tables and simplified schemas
- [ ] Document relationships centered on `PERSON.person_id`
- [ ] Document storage plan for `data/raw/` and `data/processed/`
- [ ] Document synthetic data assumptions and initial concept sets
- [ ] Define the `analytic_person` output schema and column derivations in `docs/spec.md`
- [ ] Write implementation approach in `docs/plan.md`
- [ ] Clarify what Block 1 includes versus what is deferred
- [ ] Keep `README.md` aligned with actual current scope

## Repo and environment

- [ ] Confirm top-level folders exist: `docs/`, `src/`, `tests/`, `notebooks/`, `data/`, `scripts/`
- [ ] Confirm local virtual environment `myenv` works
- [ ] Install Block 1 dependencies
- [ ] Capture pinned dependencies in `requirements.txt`
- [ ] Finalize `.gitignore` for envs, data, caches, logs, and notebook artifacts

## Schema and generation design

- [ ] Finalize exact column lists for PERSON
- [ ] Finalize exact column lists for VISIT_OCCURRENCE
- [ ] Finalize exact column lists for CONDITION_OCCURRENCE
- [ ] Finalize exact column lists for DRUG_EXPOSURE
- [ ] Finalize exact column lists for MEASUREMENT
- [ ] Define datatypes for all columns
- [ ] Define concept ID sets for visit types, conditions, drugs, and measurements
- [ ] Define generation rules for demographics
- [ ] Define generation rules for visit frequency
- [ ] Define generation rules for chronic conditions
- [ ] Define generation rules for drug exposures
- [ ] Define generation rules for measurements and realistic ranges
- [ ] Decide initial output format: CSV or Parquet

## Data generation

- [ ] Implement PERSON generator
- [ ] Implement VISIT_OCCURRENCE generator
- [ ] Implement CONDITION_OCCURRENCE generator
- [ ] Implement DRUG_EXPOSURE generator
- [ ] Implement MEASUREMENT generator
- [ ] Add deterministic seed support
- [ ] Write generated tables to `data/raw/`
- [ ] Verify row counts and key uniqueness

## Pipeline

- [ ] Implement raw-data readers in PySpark
- [ ] Add explicit schemas
- [ ] Implement type casting and date parsing
- [ ] Add basic null handling
- [ ] Add referential integrity validation on `person_id`
- [ ] Add optional referential validation on `visit_occurrence_id`
- [ ] Add date-order validation
- [ ] Implement visit aggregations by person
- [ ] Implement condition-based flags
- [ ] Implement latest-measurement logic
- [ ] Build final `analytic_person` dataset
- [ ] Write final output to `data/processed/`

## Testing

- [ ] Add `tests/conftest.py` Spark fixture
- [ ] Add unit tests for helper functions
- [ ] Add validation tests for `person_id` integrity
- [ ] Add validation tests for date consistency
- [ ] Add tests for required non-null fields
- [ ] Add transformation tests for person-level aggregations
- [ ] Add smoke test for pipeline run on tiny sample data

## Notebook demo

- [ ] Create `notebooks/block1_demo.ipynb`
- [ ] Add short markdown intro about project and dataset
- [ ] Load processed analytic output
- [ ] Show row counts and schema
- [ ] Add one or two simple analyses
- [ ] Add one or two simple plots

## Polish

- [ ] Review `.gitignore` one more time
- [ ] Review README for accuracy
- [ ] Make sure notebook output is present only if intentional
- [ ] Make sure only tiny sample data, if any, is committed
- [ ] Make sure docs reflect final Block 1 implementation
# Block 1 Specification

## Project title

Synthetic OMOP-Style Healthcare Batch Pipeline

## Goal

Build a reproducible Python/PySpark batch pipeline over a fully synthetic OMOP-style healthcare dataset.

Block 1 focuses on:
- clear project documentation
- clean repository structure
- local development environment
- synthetic data design and generation
- a basic PySpark pipeline
- initial tests and a demo notebook

Block 1 does not focus on:
- large-scale performance tuning
- orchestration or scheduling
- cloud deployment
- full OMOP vocabulary fidelity
- advanced healthcare semantics
- all OMOP tables

## Problem statement

Healthcare datasets are often modeled in a patient-centric way, where a central PERSON table is linked to clinical event tables such as visits, conditions, medications, and measurements. The OMOP Common Data Model follows this pattern, with clinical events linked back to PERSON through `person_id`. 

This project will build a simplified OMOP-style batch pipeline using synthetic data only. The goal is to create a realistic, interview-worthy foundation that demonstrates data engineering practices without relying on real patient data. 

## Dataset

Block 1 uses a simplified subset of OMOP-style tables.

### 1. PERSON

One row per synthetic patient.

Columns:
- `person_id` (int) — unique patient identifier
- `gender_concept_id` (int) — synthetic gender concept
- `year_of_birth` (int)
- `race_concept_id` (int)
- `ethnicity_concept_id` (int)
- `location_id` (int, nullable)

### 2. VISIT_OCCURRENCE

Represents encounters such as outpatient, inpatient, or emergency visits. OMOP defines visit occurrences as spans of time during which a person receives healthcare services in a setting. 

Columns:
- `visit_occurrence_id` (int) — unique visit identifier
- `person_id` (int) — foreign key to PERSON
- `visit_concept_id` (int) — synthetic visit type concept
- `visit_start_date` (date)
- `visit_end_date` (date)
- `care_site_id` (int, nullable)
- `provider_id` (int, nullable)

### 3. CONDITION_OCCURRENCE

Represents diagnoses or conditions observed for a person.

Columns:
- `condition_occurrence_id` (int)
- `person_id` (int) — foreign key to PERSON
- `condition_concept_id` (int)
- `condition_start_date` (date)
- `condition_end_date` (date, nullable)
- `visit_occurrence_id` (int, nullable)

### 4. DRUG_EXPOSURE

Represents medication exposure records such as orders or administrations.

Columns:
- `drug_exposure_id` (int)
- `person_id` (int) — foreign key to PERSON
- `drug_concept_id` (int)
- `drug_exposure_start_date` (date)
- `drug_exposure_end_date` (date, nullable)
- `days_supply` (int)
- `quantity` (double)

### 5. MEASUREMENT

Represents labs and vitals. In OMOP, measurement records include numeric results tied to a person and optionally to a visit.

Columns:
- `measurement_id` (int)
- `person_id` (int) — foreign key to PERSON
- `measurement_concept_id` (int)
- `measurement_date` (date)
- `value_as_number` (double)
- `unit_concept_id` (int, nullable)
- `visit_occurrence_id` (int, nullable)

### 6. NOTE

Synthetic clinical note text. This table is included to support a future RAG (Retrieval-Augmented Generation) project, where clinical notes can be embedded and retrieved for patient-specific queries.

Columns:
- `note_id` (int) — unique note identifier
- `person_id` (int) — foreign key to PERSON
- `note_date` (date)
- `note_text` (string)
- `visit_occurrence_id` (int, nullable) — foreign key to VISIT_OCCURRENCE

## Relationships

This dataset is patient-centric:
- `PERSON.person_id` is the central key.
- All event tables reference `PERSON.person_id`.
- Some event tables may also reference `VISIT_OCCURRENCE.visit_occurrence_id`.

Key integrity expectations:
- every event row must map to a valid `person_id`
- if `visit_occurrence_id` is populated, it must map to a valid visit
- date ranges must be logically ordered

## Data Lineage

Synthea CLI (Java) → `data/synthea_raw/` (native Synthea CSV export)
        ↓ (`src/generator.py`: concept-dictionary mapping, hardcoded NOTE generation, dirty-data injection)
`data/raw/` (simplified OMOP-style tables, ~1-2% intentionally dirty rows per category)
        ↓
Validation (detection: null / datatype / range / referential-integrity / date-order checks)
        ↓
Cleaning + Transformations (drop/quarantine dirty rows, joins, aggregations)
        ↓
Validation (post-cleaning gate — abort if unresolved issues remain)
        ↓
analytic_person → `data/processed/` (partitioned Parquet)

## Storage plan

Local data layout:
- raw Synthea export: `data/synthea_raw/` — native Synthea CSV output (`patients.csv`, `encounters.csv`, `conditions.csv`, `medications.csv`, `observations.csv`, etc.)
- raw generated data: `data/raw/` (simplified OMOP-style tables produced by `src/generator.py`, including intentionally injected dirty rows)
- processed outputs: `data/processed/`
- optional tiny sample data for tests: `data/sample/`

The `data/synthea_raw/`, `data/raw/` and `data/processed/` directories are git-ignored so no bulk synthetic data is committed to source control. Only generator code, schemas, concept dictionaries, and tiny test samples may live in git. 

## Synthetic data assumptions

All data in this project is fully synthetic and de-identified by design. Primary synthetic data source is Synthea. 

Raw Synthea outputs are transformed into a simplified OMOP-style model
used throughout the project.

Where needed, additional synthetic records may be generated to support
testing or scale experiments. 

Generation rules will aim for internal consistency and basic realism, not medical completeness. Initial rules include:
- realistic age distribution from `year_of_birth`
- more outpatient visits than inpatient or emergency visits
- chronic and acute condition patterns
- drug records loosely associated with selected chronic conditions
- measurement values within plausible ranges for selected labs and vitals

Examples of synthetic concept families:
- visit types: outpatient, inpatient, ER
- chronic conditions: diabetes, hypertension, hyperlipidemia
- measurements: systolic blood pressure, BMI, glucose, HbA1c

### Concept ID mapping

Synthea emits real-world codes (SNOMED for conditions and visit types, RxNorm for drugs, LOINC for measurements) and string fields (e.g., gender as `"M"`/`"F"`). Block 1 does not aim for full OMOP vocabulary fidelity, so `src/concepts.py` defines a curated whitelist that maps the Synthea codes for the concept families above to small synthetic integer `*_concept_id` values. Records whose Synthea codes fall outside this whitelist are excluded by `src/generator.py` rather than mapped.

### Identifier mapping

Synthea's raw CSV export (`data/synthea_raw/`) identifies patients, encounters, and other records with UUIDs. `src/generator.py` maintains in-memory UUID-to-integer lookups (one per entity: patients map to `person_id`, encounters map to `visit_occurrence_id`) while mapping rows into the simplified tables, so every `*_id` column in `data/raw/` is the synthetic `int` defined above — no UUIDs are persisted past the generation step.

### NOTE generation

Synthea's CSV export does not include free-text clinical notes. `src/generator.py` generates synthetic note text by randomly selecting from hardcoded complaint and assessment phrases keyed to the visit type (outpatient, inpatient, ER). Each visit produces one NOTE record with a `note_text` built from these canned strings. The remaining columns — `note_id` (sequential), `person_id`, `note_date` (from `visit_start_date`), and `visit_occurrence_id` — are derived directly from the mapped VISIT_OCCURRENCE rows.

### Target scale

`src/generator.py` targets roughly 10,000 PERSON rows, tuned so the combined row count across all six tables stays under roughly 100,000.

## Analytic output

Block 1 will produce one person-level analytic dataset under `data/processed/`.

Planned output table: `analytic_person`

Planned columns:
- `person_id`
- `age`
- `gender_concept_id`
- `total_visit_count`
- `outpatient_visit_count`
- `inpatient_visit_count`
- `er_visit_count`
- `condition_count`
- `drug_exposure_count`
- `measurement_count`
- `has_diabetes`
- `has_hypertension`
- `latest_hba1c`
- `latest_systolic_bp`
- `latest_measurement_date`

Initial derivation ideas:
- age derived using a fixed reference date (e.g., 2025-01-01) to ensure reproducible outputs.
- visit counts aggregated from `VISIT_OCCURRENCE`
- chronic condition flags derived from `CONDITION_OCCURRENCE`
- latest measurements derived using most recent `measurement_date`

## Intentional data quality issues

To exercise the validation and cleaning steps, `src/generator.py` deterministically injects a small, known fraction (~1-2% per category) of "dirty" rows into `data/raw/`:
- duplicate rows / duplicate primary keys
- nulls in fields the schema treats as required
- illogical date pairs (end date before start date)
- out-of-range numeric values (e.g., negative `days_supply`/`quantity`, implausible `value_as_number`)
- orphaned `person_id` / `visit_occurrence_id` references

These are detected by `src/validations.py` and removed/quarantined by `src/transforms.py` (see Functional requirements 10 and 13).

## Functional requirements

Block 1 must:
1. Generate synthetic OMOP-style source tables locally.
2. Write raw outputs to `data/raw/`.
3. Read raw data into PySpark.
4. Apply basic validation and cleaning.
5. Join tables around `person_id` and, when needed, `visit_occurrence_id`.
6. Produce a person-level analytic output dataset.
7. Write processed output to `data/processed/`.
8. Include tests for key constraints and transformation logic.
9. Include a notebook demo that loads and explores the final analytic output.
10. Capture and log pipeline operational metrics:
        - raw row counts(before cleaning and after cleaning)
        - processed row counts
        - validation failures
        - total runtime duration
        - validation duration
        - transformation duration
11. The entire Block 1 workflow must execute from a single command.
Example flow:
        Generate Data
        → Validate Data
        → Transform Data
        → Build analytic_person
        → Write Processed Output
        → Write Pipeline Metrics
12. Processed output must be written as partitioned Parquet.
Example:
analytic_person/
    year_of_birth_band=1940s/
    year_of_birth_band=1950s/
13. Validation failures must fail the pipeline and prevent output creation.
Validation categories:
        - null checks
        - datatype checks
        - value-range checks
        - referential integrity checks

Note: validation runs twice. An initial detection pass on raw data feeds the cleaning step (and the "validation failures" metric in requirement 10) without halting the pipeline, since `data/raw/` intentionally contains a small share of dirty rows (see "Intentional data quality issues"). A second pass on the cleaned data is the hard gate — if issues remain after cleaning, the pipeline fails and no processed output is written.

## Success criteria
Block 1 is complete when:
* docs are present and aligned with implementation
* the solution is implemented using Python and PySpark
* raw synthetic data can be generated locally
* the PySpark pipeline runs end-to-end
* the pipeline executes from a single command
* the person-level analytic dataset is created successfully
* processed output passes schema validation checks
* null, datatype, range and referential integrity validations pass
* validation failures prevent output creation and fail the pipeline
* at least one join is implemented in the pipeline
* at least one aggregation is implemented in the pipeline
* processed output is written as partitioned Parquet
* before-cleaning and after-cleaning row counts are logged
* runtime metrics are captured
* pipeline metrics are logged and reviewable
* tests pass on sample data
* the notebook demo can load and inspect the processed output
* README contains a pipeline architecture diagram
* pipeline produces identical output when re-run with the same seed and reference date

# Synthetic OMOP-Style Healthcare Batch Pipeline

A Python/PySpark batch pipeline project built on a fully synthetic OMOP-style healthcare dataset. The goal of Block 1 is to establish a clean, testable, and interview-ready foundation for a healthcare data pipeline using a simplified subset of OMOP-like tables. 

## Current scope

Block 1 includes:
- project documentation (`spec.md`, `plan.md`, `tasks.md`)
- synthetic OMOP-style data design
- local synthetic data generation
- a basic PySpark batch pipeline
- initial tests with `pytest`
- a demo notebook

Block 1 does not yet include:
- large-scale performance tuning
- orchestration
- cloud deployment
- full OMOP vocabulary mapping
- advanced healthcare semantics

## Tech stack

- Python
- PySpark
- pytest
- Jupyter Notebook or JupyterLab

## Project structure

```text
docs/        project specification, plan, and tasks
src/         pipeline modules and helper code
tests/       pytest-based tests
notebooks/   demo notebook for Block 1
scripts/     utility scripts and wrappers
data/synthea_raw/ local raw Synthea export (git-ignored)
data/raw/    local raw synthetic data (git-ignored)
data/processed/ local processed outputs (git-ignored)
```

## Setup

Prerequisites:
- Python 3.11+
- Java 11+ (required once, to run Synthea and produce the raw patient export)

1. Create and activate a virtual environment.
2. Install dependencies from `requirements.txt`.
3. Run Synthea (via `scripts/run_synthea.ps1`) to generate a raw patient export into `data/synthea_raw/`.
4. Run the Block 1 pipeline — generates the simplified `data/raw/` tables from the Synthea export, validates, cleans, transforms, and writes `data/processed/`.
5. Run tests.
6. Open the notebook demo.

Example setup:

```bash
python -m venv myenv
source myenv/bin/activate
pip install -r requirements.txt
```

## Planned commands

Examples of planned local commands:

```bash
./scripts/run_synthea.ps1     # one-time: generate data/synthea_raw/ (requires Java)
python -m src.main             # generate data/raw/, validate, clean, transform, write data/processed/
pytest
jupyter notebook
```

## Data note

This project uses synthetic OMOP-style healthcare data only. Bulk generated data is stored locally and is not committed to version control.

## Status

Block 1 is focused on foundation and structure first. Later blocks may expand the schema, increase scale, and introduce more advanced engineering concerns.
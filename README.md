# GenAI Block 1 Batch Pipeline

A Python/PySpark batch data pipeline project for processing person-level data and building a clean, testable analytics workflow. This repository is being developed in stages, with Block 1 focused on project setup, documentation, and initial pipeline scaffolding.

## Overview

The goal of this project is to build a reproducible batch processing pipeline using Python and PySpark, supported by tests and clear project documentation. The repository is structured to separate reusable source code, tests, notebooks, and planning documents.

## Current Scope

Block 1 currently includes:
- project scaffold
- setup documentation
- software design documents
- Python environment configuration
- dependency tracking with `requirements.txt`

## Tech Stack

- Python
- PySpark
- pytest
- Jupyter Notebook
- Git / GitHub

## Project Structure

```text
genai-block1-batch-pipeline/
├── docs/
├── data/
├── notebooks/
├── src/
├── tests/
├── scripts/
├── requirements.txt
├── .gitignore
└── README.md
```

## Setup

Create and activate the virtual environment:

```powershell
python -m venv myenv
.\myenv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

## How to Run

Launch Jupyter Notebook:

```powershell
jupyter notebook
```

Run tests:

```powershell
pytest
```

## Documentation

Project planning and design documents are located in the `docs/` folder.

## Next Steps

- implement initial PySpark pipeline logic
- add notebook-based demonstration
- expand unit tests
- add sample input/output workflow

## Status

Pending mentor review and approval before the next implementation phase.
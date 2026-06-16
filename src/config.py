"""Project-wide configuration constants and paths for Block 1.

See docs/spec.md and docs/plan.md for the rationale behind these values.
"""

from datetime import date
from pathlib import Path

# Reproducibility
REFERENCE_DATE = date(2025, 1, 1)
RANDOM_SEED = 42

# Target scale (see docs/spec.md "Target scale")
NUM_PERSONS = 10_000
TOTAL_ROW_BUDGET = 100_000

# Max visits kept per person (controls row budget at scale; see docs/spec.md)
VISITS_PER_PERSON = 2

# Share of rows injected with each dirty-data issue category (Phase 5)
DIRTY_DATA_FRACTION = 0.015

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SYNTHEA_RAW_DIR = DATA_DIR / "synthea_raw"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
SAMPLE_DIR = DATA_DIR / "sample"

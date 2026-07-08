"""Project-wide configuration constants and paths for Block 1.

See docs/spec.md and docs/plan.md for the rationale behind these values.
"""

import os
from datetime import date
from pathlib import Path

# Reproducibility — overridable via env vars so scripts/run_all.py's
# --seed/--reference-date flags actually control these instead of being
# silently ignored by everything downstream of Synthea.
REFERENCE_DATE = date.fromisoformat(os.environ.get("BLOCK1_REFERENCE_DATE", "2025-01-01"))
RANDOM_SEED = int(os.environ.get("BLOCK1_RANDOM_SEED", "42"))

# Target scale (see docs/spec.md "Target scale")
NUM_PERSONS = 10_000
TOTAL_ROW_BUDGET = 100_000

# Max visits kept per person (controls row budget at scale; see docs/spec.md)
VISITS_PER_PERSON = 2

# Share of rows injected with each dirty-data issue category (Phase 5)
DIRTY_DATA_FRACTION = 0.015

# Fallback values used when Synthea's medication dates/dispense counts are
# missing (see src/generator.py::_map_drug_exposure)
DEFAULT_DAYS_SUPPLY = 30   # assumed days_supply when start/stop dates can't be diffed
MIN_DAYS_SUPPLY = 1        # clamp floor — avoids 0-day dispenses
DEFAULT_QUANTITY = 1.0     # assumed quantity when DISPENSES is missing/non-numeric

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SYNTHEA_RAW_DIR = DATA_DIR / "synthea_raw"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
SAMPLE_DIR = DATA_DIR / "sample"

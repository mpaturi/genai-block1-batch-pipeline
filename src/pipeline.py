"""
Main pipeline orchestration for the Block 1 OMOP batch pipeline.

Execution order
---------------
1. Read raw CSVs from data/raw/ via io_utils.
2. validate_all() on raw tables  — detection only; results logged, pipeline continues.
3. clean_all()                   — drop dirty rows, return CleanedTables + CleaningMetrics.
4. validate_all() on cleaned     — hard gate; raises PipelineValidationError if any
                                   violations remain after cleaning.
5. build_analytic_person()       — produce one row per person.
6. write_parquet()               — write analytic_person to data/processed/ partitioned
                                   by year_of_birth_band.
7. write_metrics()               — persist row counts, validation results, and stage
                                   timings to data/processed/pipeline_metrics.json.

Entry point: run() — call directly or via `python -m src.pipeline`.
"""

import json
import logging
import sys
import time

from src import io_utils, transforms, validations
from src.config import PROCESSED_DIR
from src.transforms import CleanedTables
from src.validations import ValidationResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


class PipelineValidationError(RuntimeError):
    """Raised when cleaned tables still contain validation violations."""


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def _log_validation_results(results: list[ValidationResult], stage: str) -> None:
    violations = [r for r in results if r.count > 0]
    if not violations:
        log.info("[%s] All validation checks passed (0 violations)", stage)
        return
    log.warning("[%s] %d check(s) with violations:", stage, len(violations))
    for r in violations:
        log.warning("  %-30s  %-35s  count=%d", r.table, r.check, r.count)


def _log_cleaning_metrics(metrics) -> None:
    log.info("Cleaning summary (rows before → after):")
    for table, before in metrics.before.items():
        after = metrics.after[table]
        dropped = before - after
        log.info("  %-30s  %6d → %6d  (dropped %d)", table, before, after, dropped)


def _validation_to_dict(results: list[ValidationResult]) -> list[dict]:
    return [
        {"table": r.table, "check": r.check, "violations": r.count}
        for r in results
    ]


def _write_metrics(metrics_dict: dict) -> None:
    path = PROCESSED_DIR / "pipeline_metrics.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics_dict, f, indent=2)
    log.info("Pipeline metrics written to %s", path)


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def _read_raw(spark) -> tuple:
    log.info("Reading raw CSVs from data/raw/ ...")
    person      = io_utils.read_person(spark)
    visit       = io_utils.read_visit_occurrence(spark)
    condition   = io_utils.read_condition_occurrence(spark)
    drug        = io_utils.read_drug_exposure(spark)
    measurement = io_utils.read_measurement(spark)
    note        = io_utils.read_note(spark)
    return person, visit, condition, drug, measurement, note


def _validate_raw(person, visit, condition, drug, measurement, note) -> list[ValidationResult]:
    log.info("Running validation on raw tables ...")
    results = validations.validate_all(person, visit, condition, drug, measurement, note)
    _log_validation_results(results, "RAW")
    return results


def _clean(person, visit, condition, drug, measurement, note) -> tuple[CleanedTables, object]:
    log.info("Cleaning tables ...")
    tables, metrics = transforms.clean_all(person, visit, condition, drug, measurement, note)
    _log_cleaning_metrics(metrics)
    return tables, metrics


def _validate_cleaned(tables: CleanedTables) -> list[ValidationResult]:
    log.info("Running validation on cleaned tables (hard gate) ...")
    results = validations.validate_all(
        tables.person,
        tables.visit,
        tables.condition,
        tables.drug,
        tables.measurement,
        tables.note,
    )
    violations = [r for r in results if r.count > 0]
    _log_validation_results(results, "CLEANED")
    if violations:
        summary = ", ".join(f"{r.table}.{r.check}={r.count}" for r in violations)
        raise PipelineValidationError(
            f"Cleaned tables still have {len(violations)} violation(s): {summary}"
        )
    return results


def _build_and_write(tables: CleanedTables, spark) -> int:
    log.info("Building analytic_person ...")
    analytic = transforms.build_analytic_person(
        tables.person,
        tables.visit,
        tables.condition,
        tables.drug,
        tables.measurement,
    )
    log.info("Writing analytic_person to %s (partitioned by year_of_birth_band) ...", PROCESSED_DIR)
    io_utils.write_parquet(analytic, PROCESSED_DIR, partition_by=["year_of_birth_band"])
    row_count = analytic.count()
    log.info("Done. Row count: %d", row_count)
    return row_count


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run() -> None:
    spark = io_utils.get_spark_session("block1-pipeline")
    try:
        t_start = time.perf_counter()

        person, visit, condition, drug, measurement, note = _read_raw(spark)

        t_val_raw = time.perf_counter()
        raw_results = _validate_raw(person, visit, condition, drug, measurement, note)
        t_val_raw_done = time.perf_counter()

        t_clean = time.perf_counter()
        tables, cleaning_metrics = _clean(person, visit, condition, drug, measurement, note)
        t_clean_done = time.perf_counter()

        t_val_clean = time.perf_counter()
        clean_results = _validate_cleaned(tables)
        t_val_clean_done = time.perf_counter()

        t_build = time.perf_counter()
        analytic_count = _build_and_write(tables, spark)
        t_build_done = time.perf_counter()

        t_total = time.perf_counter() - t_start

        _write_metrics({
            "row_counts": {
                "raw": cleaning_metrics.before,
                "cleaned": cleaning_metrics.after,
                "dropped": {
                    t: cleaning_metrics.before[t] - cleaning_metrics.after[t]
                    for t in cleaning_metrics.before
                },
                "analytic_person": analytic_count,
            },
            "validation": {
                "raw": _validation_to_dict(raw_results),
                "cleaned": _validation_to_dict(clean_results),
            },
            "timings_seconds": {
                "validation_raw": round(t_val_raw_done - t_val_raw, 2),
                "cleaning": round(t_clean_done - t_clean, 2),
                "validation_cleaned": round(t_val_clean_done - t_val_clean, 2),
                "build_and_write": round(t_build_done - t_build, 2),
                "total": round(t_total, 2),
            },
        })

        log.info("Pipeline completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        run()
    except PipelineValidationError as exc:
        log.error("Pipeline aborted: %s", exc)
        sys.exit(1)

"""
Detection-only validation for all six raw OMOP-style tables.

Each validate_*() function scans one table and returns a list of ValidationResult
instances — one per check — with a violation count. No rows are dropped here;
that is transforms.py's responsibility.

All checks for a given table are batched into a single Spark aggregation to
minimize the number of jobs.

Checks performed per table
--------------------------
PERSON              : null required fields, duplicate person_id
VISIT_OCCURRENCE    : null required fields, end<start date, orphan person_id, duplicate PK
CONDITION_OCCURRENCE: null required fields, end<start date, duplicate PK
DRUG_EXPOSURE       : null required fields, end<start date, negative days_supply/quantity, duplicate PK
MEASUREMENT         : null required fields, negative value_as_number, orphan person_id, duplicate PK
NOTE                : null required fields, orphan visit_occurrence_id, duplicate PK
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class ValidationResult:
    table: str
    check: str
    count: int


def _count_expr(check_name: str, condition):
    return F.sum(F.when(condition, 1).otherwise(0)).alias(check_name)


def _null_expr(col: str):
    return _count_expr(f"null_{col}", F.col(col).isNull())


def _neg_expr(col: str):
    return _count_expr(f"neg_{col}", F.col(col) < 0)


def _bad_date_expr(start_col: str, end_col: str):
    return _count_expr(
        f"bad_date_{end_col}",
        F.col(end_col).isNotNull() & (F.col(end_col) < F.col(start_col)),
    )


def _dup_count(df: DataFrame, pk: str) -> int:
    dup_counts = df.groupBy(pk).count().filter(F.col("count") > 1)
    total = dup_counts.select(F.sum(F.col("count") - 1)).collect()[0][0]
    return int(total or 0)


def _orphan_count(df: DataFrame, fk_col: str, parent: DataFrame, parent_pk: str) -> int:
    parent_ids = parent.select(F.col(parent_pk).alias("_parent_pk")).distinct()
    bad = (
        df.filter(F.col(fk_col).isNotNull())
          .join(parent_ids, df[fk_col] == parent_ids["_parent_pk"], "left_anti")
    )
    return bad.count()


def _batch_validate(df: DataFrame, table: str, pk: str, exprs: list,
                    extra_results: list[ValidationResult] | None = None) -> list[ValidationResult]:
    row = df.agg(*exprs).collect()[0]
    results = [ValidationResult(table, name, int(row[name])) for name in row.asDict()]
    if extra_results:
        results.extend(extra_results)
    return results


# ---------------------------------------------------------------------------
# Per-table validation functions
# ---------------------------------------------------------------------------

def validate_person(df: DataFrame) -> list[ValidationResult]:
    exprs = [
        _null_expr("gender_concept_id"),
        _null_expr("year_of_birth"),
        _null_expr("race_concept_id"),
        _null_expr("ethnicity_concept_id"),
    ]
    return _batch_validate(df, "person", "person_id", exprs, [
        ValidationResult("person", "dup_pk", _dup_count(df, "person_id")),
    ])


def validate_visit_occurrence(df: DataFrame, person: DataFrame) -> list[ValidationResult]:
    exprs = [
        _null_expr("person_id"),
        _null_expr("visit_concept_id"),
        _null_expr("visit_start_date"),
        _null_expr("visit_end_date"),
        _bad_date_expr("visit_start_date", "visit_end_date"),
    ]
    return _batch_validate(df, "visit_occurrence", "visit_occurrence_id", exprs, [
        ValidationResult("visit_occurrence", "orphan_person_id",
                         _orphan_count(df, "person_id", person, "person_id")),
        ValidationResult("visit_occurrence", "dup_pk",
                         _dup_count(df, "visit_occurrence_id")),
    ])


def validate_condition_occurrence(df: DataFrame) -> list[ValidationResult]:
    exprs = [
        _null_expr("person_id"),
        _null_expr("condition_concept_id"),
        _null_expr("condition_start_date"),
        _bad_date_expr("condition_start_date", "condition_end_date"),
    ]
    return _batch_validate(df, "condition_occurrence", "condition_occurrence_id", exprs, [
        ValidationResult("condition_occurrence", "dup_pk",
                         _dup_count(df, "condition_occurrence_id")),
    ])


def validate_drug_exposure(df: DataFrame) -> list[ValidationResult]:
    exprs = [
        _null_expr("person_id"),
        _null_expr("drug_concept_id"),
        _null_expr("drug_exposure_start_date"),
        _null_expr("days_supply"),
        _null_expr("quantity"),
        _bad_date_expr("drug_exposure_start_date", "drug_exposure_end_date"),
        _neg_expr("days_supply"),
        _neg_expr("quantity"),
    ]
    return _batch_validate(df, "drug_exposure", "drug_exposure_id", exprs, [
        ValidationResult("drug_exposure", "dup_pk",
                         _dup_count(df, "drug_exposure_id")),
    ])


def validate_measurement(df: DataFrame, person: DataFrame) -> list[ValidationResult]:
    exprs = [
        _null_expr("person_id"),
        _null_expr("measurement_concept_id"),
        _null_expr("measurement_date"),
        _null_expr("value_as_number"),
        _neg_expr("value_as_number"),
    ]
    return _batch_validate(df, "measurement", "measurement_id", exprs, [
        ValidationResult("measurement", "orphan_person_id",
                         _orphan_count(df, "person_id", person, "person_id")),
        ValidationResult("measurement", "dup_pk",
                         _dup_count(df, "measurement_id")),
    ])


def validate_note(df: DataFrame, visit: DataFrame) -> list[ValidationResult]:
    exprs = [
        _null_expr("person_id"),
        _null_expr("note_date"),
        _null_expr("note_text"),
    ]
    return _batch_validate(df, "note", "note_id", exprs, [
        ValidationResult("note", "orphan_visit_occurrence_id",
                         _orphan_count(df, "visit_occurrence_id", visit, "visit_occurrence_id")),
        ValidationResult("note", "dup_pk",
                         _dup_count(df, "note_id")),
    ])


# ---------------------------------------------------------------------------
# Aggregate entry point
# ---------------------------------------------------------------------------

def validate_all(
    person: DataFrame,
    visit: DataFrame,
    condition: DataFrame,
    drug: DataFrame,
    measurement: DataFrame,
    note: DataFrame,
) -> list[ValidationResult]:
    """Run all checks across all six tables and return the combined results."""
    return (
        validate_person(person)
        + validate_visit_occurrence(visit, person)
        + validate_condition_occurrence(condition)
        + validate_drug_exposure(drug)
        + validate_measurement(measurement, person)
        + validate_note(note, visit)
    )

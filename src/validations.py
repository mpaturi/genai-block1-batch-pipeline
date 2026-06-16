"""
Detection-only validation for all six raw OMOP-style tables.

Each validate_*() function scans one table and returns a list of ValidationResult
instances — one per check — with a violation count and (for most checks) the
primary-key values of the offending rows.  No rows are dropped here; that is
transforms.py's responsibility.

Checks performed per table
--------------------------
PERSON              : null required fields, duplicate person_id
VISIT_OCCURRENCE    : null required fields, end<start date, orphan person_id, duplicate PK
CONDITION_OCCURRENCE: null required fields, end<start date, duplicate PK
DRUG_EXPOSURE       : null required fields, end<start date, negative days_supply/quantity, duplicate PK
MEASUREMENT         : null required fields, negative value_as_number, orphan person_id, duplicate PK
NOTE                : null required fields, orphan visit_occurrence_id, duplicate PK
"""

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class ValidationResult:
    table: str
    check: str
    count: int
    bad_ids: list = field(default_factory=list)  # PKs of violating rows; empty for dup_pk


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _null_check(df: DataFrame, table: str, pk: str, col: str) -> ValidationResult:
    bad = df.filter(F.col(col).isNull())
    ids = [row[pk] for row in bad.select(pk).collect()]
    return ValidationResult(table, f"null_{col}", len(ids), ids)


def _dup_check(df: DataFrame, table: str, pk: str) -> ValidationResult:
    """Count extra rows created by duplicate primary-key values."""
    dup_counts = df.groupBy(pk).count().filter(F.col("count") > 1)
    total_extras = dup_counts.select(F.sum(F.col("count") - 1)).collect()[0][0]
    return ValidationResult(table, "dup_pk", int(total_extras or 0))


def _date_order_check(
    df: DataFrame, table: str, pk: str, start_col: str, end_col: str
) -> ValidationResult:
    """Flag rows where end_date < start_date (only when end_date is not null)."""
    bad = df.filter(F.col(end_col).isNotNull() & (F.col(end_col) < F.col(start_col)))
    ids = [row[pk] for row in bad.select(pk).collect()]
    return ValidationResult(table, f"bad_date_{end_col}", len(ids), ids)


def _neg_check(df: DataFrame, table: str, pk: str, col: str) -> ValidationResult:
    bad = df.filter(F.col(col) < 0)
    ids = [row[pk] for row in bad.select(pk).collect()]
    return ValidationResult(table, f"neg_{col}", len(ids), ids)


def _orphan_check(
    df: DataFrame,
    table: str,
    pk: str,
    fk_col: str,
    parent: DataFrame,
    parent_pk: str,
) -> ValidationResult:
    """Flag non-null FK values not present in the parent table."""
    parent_ids = parent.select(F.col(parent_pk).alias("_parent_pk")).distinct()
    bad = (
        df.filter(F.col(fk_col).isNotNull())
          .join(parent_ids, df[fk_col] == parent_ids["_parent_pk"], "left_anti")
    )
    ids = [row[pk] for row in bad.select(pk).collect()]
    return ValidationResult(table, f"orphan_{fk_col}", len(ids), ids)


# ---------------------------------------------------------------------------
# Per-table validation functions
# ---------------------------------------------------------------------------

def validate_person(df: DataFrame) -> list[ValidationResult]:
    return [
        _null_check(df, "person", "person_id", "gender_concept_id"),
        _null_check(df, "person", "person_id", "year_of_birth"),
        _null_check(df, "person", "person_id", "race_concept_id"),
        _null_check(df, "person", "person_id", "ethnicity_concept_id"),
        _dup_check(df, "person", "person_id"),
    ]


def validate_visit_occurrence(df: DataFrame, person: DataFrame) -> list[ValidationResult]:
    return [
        _null_check(df, "visit_occurrence", "visit_occurrence_id", "person_id"),
        _null_check(df, "visit_occurrence", "visit_occurrence_id", "visit_concept_id"),
        _null_check(df, "visit_occurrence", "visit_occurrence_id", "visit_start_date"),
        _null_check(df, "visit_occurrence", "visit_occurrence_id", "visit_end_date"),
        _date_order_check(df, "visit_occurrence", "visit_occurrence_id",
                          "visit_start_date", "visit_end_date"),
        _orphan_check(df, "visit_occurrence", "visit_occurrence_id",
                      "person_id", person, "person_id"),
        _dup_check(df, "visit_occurrence", "visit_occurrence_id"),
    ]


def validate_condition_occurrence(df: DataFrame) -> list[ValidationResult]:
    return [
        _null_check(df, "condition_occurrence", "condition_occurrence_id", "person_id"),
        _null_check(df, "condition_occurrence", "condition_occurrence_id", "condition_concept_id"),
        _null_check(df, "condition_occurrence", "condition_occurrence_id", "condition_start_date"),
        _date_order_check(df, "condition_occurrence", "condition_occurrence_id",
                          "condition_start_date", "condition_end_date"),
        _dup_check(df, "condition_occurrence", "condition_occurrence_id"),
    ]


def validate_drug_exposure(df: DataFrame) -> list[ValidationResult]:
    return [
        _null_check(df, "drug_exposure", "drug_exposure_id", "person_id"),
        _null_check(df, "drug_exposure", "drug_exposure_id", "drug_concept_id"),
        _null_check(df, "drug_exposure", "drug_exposure_id", "drug_exposure_start_date"),
        _null_check(df, "drug_exposure", "drug_exposure_id", "days_supply"),
        _null_check(df, "drug_exposure", "drug_exposure_id", "quantity"),
        _date_order_check(df, "drug_exposure", "drug_exposure_id",
                          "drug_exposure_start_date", "drug_exposure_end_date"),
        _neg_check(df, "drug_exposure", "drug_exposure_id", "days_supply"),
        _neg_check(df, "drug_exposure", "drug_exposure_id", "quantity"),
        _dup_check(df, "drug_exposure", "drug_exposure_id"),
    ]


def validate_measurement(df: DataFrame, person: DataFrame) -> list[ValidationResult]:
    return [
        _null_check(df, "measurement", "measurement_id", "person_id"),
        _null_check(df, "measurement", "measurement_id", "measurement_concept_id"),
        _null_check(df, "measurement", "measurement_id", "measurement_date"),
        _null_check(df, "measurement", "measurement_id", "value_as_number"),
        _neg_check(df, "measurement", "measurement_id", "value_as_number"),
        _orphan_check(df, "measurement", "measurement_id", "person_id", person, "person_id"),
        _dup_check(df, "measurement", "measurement_id"),
    ]


def validate_note(df: DataFrame, visit: DataFrame) -> list[ValidationResult]:
    return [
        _null_check(df, "note", "note_id", "person_id"),
        _null_check(df, "note", "note_id", "note_date"),
        _null_check(df, "note", "note_id", "note_text"),
        _orphan_check(df, "note", "note_id",
                      "visit_occurrence_id", visit, "visit_occurrence_id"),
        _dup_check(df, "note", "note_id"),
    ]


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

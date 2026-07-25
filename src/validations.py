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


def _dup_count(df: DataFrame, pk: str) -> int:
    dup_counts = df.groupBy(pk).count().filter(F.col("count") > 1)
    total = dup_counts.select(F.sum(F.col("count") - 1)).collect()[0][0]
    return int(total or 0)


# Uses left_anti directly since we only want the orphan rows themselves (for counting).
def _orphan_count(df: DataFrame, fk_col: str, parent: DataFrame, parent_pk: str) -> int:
    parent_ids = parent.select(F.col(parent_pk).alias("_parent_pk")).distinct()
    bad = (
        df.filter(F.col(fk_col).isNotNull())
          .join(parent_ids, df[fk_col] == parent_ids["_parent_pk"], "left_anti")
    )
    return bad.count()


# ---------------------------------------------------------------------------
# Per-table validation functions
# ---------------------------------------------------------------------------

def validate_person(df: DataFrame) -> list[ValidationResult]:
    row = df.agg(
        F.sum(F.when(F.col("gender_concept_id").isNull(), 1).otherwise(0)).alias("null_gender_concept_id"),
        F.sum(F.when(F.col("year_of_birth").isNull(), 1).otherwise(0)).alias("null_year_of_birth"),
        F.sum(F.when(F.col("race_concept_id").isNull(), 1).otherwise(0)).alias("null_race_concept_id"),
        F.sum(F.when(F.col("ethnicity_concept_id").isNull(), 1).otherwise(0)).alias("null_ethnicity_concept_id"),
    ).collect()[0]
    return [
        ValidationResult("person", "null_gender_concept_id", int(row["null_gender_concept_id"])),
        ValidationResult("person", "null_year_of_birth", int(row["null_year_of_birth"])),
        ValidationResult("person", "null_race_concept_id", int(row["null_race_concept_id"])),
        ValidationResult("person", "null_ethnicity_concept_id", int(row["null_ethnicity_concept_id"])),
        ValidationResult("person", "dup_pk", _dup_count(df, "person_id")),
    ]


def validate_visit_occurrence(df: DataFrame, person: DataFrame) -> list[ValidationResult]:
    row = df.agg(
        F.sum(F.when(F.col("person_id").isNull(), 1).otherwise(0)).alias("null_person_id"),
        F.sum(F.when(F.col("visit_concept_id").isNull(), 1).otherwise(0)).alias("null_visit_concept_id"),
        F.sum(F.when(F.col("visit_start_date").isNull(), 1).otherwise(0)).alias("null_visit_start_date"),
        F.sum(F.when(F.col("visit_end_date").isNull(), 1).otherwise(0)).alias("null_visit_end_date"),
        F.sum(F.when(
            F.col("visit_end_date").isNotNull() & (F.col("visit_end_date") < F.col("visit_start_date")), 1
        ).otherwise(0)).alias("bad_date_visit_end_date"),
    ).collect()[0]
    return [
        ValidationResult("visit_occurrence", "null_person_id", int(row["null_person_id"])),
        ValidationResult("visit_occurrence", "null_visit_concept_id", int(row["null_visit_concept_id"])),
        ValidationResult("visit_occurrence", "null_visit_start_date", int(row["null_visit_start_date"])),
        ValidationResult("visit_occurrence", "null_visit_end_date", int(row["null_visit_end_date"])),
        ValidationResult("visit_occurrence", "bad_date_visit_end_date", int(row["bad_date_visit_end_date"])),
        ValidationResult("visit_occurrence", "orphan_person_id", _orphan_count(df, "person_id", person, "person_id")),
        ValidationResult("visit_occurrence", "dup_pk", _dup_count(df, "visit_occurrence_id")),
    ]


def validate_condition_occurrence(df: DataFrame) -> list[ValidationResult]:
    row = df.agg(
        F.sum(F.when(F.col("person_id").isNull(), 1).otherwise(0)).alias("null_person_id"),
        F.sum(F.when(F.col("condition_concept_id").isNull(), 1).otherwise(0)).alias("null_condition_concept_id"),
        F.sum(F.when(F.col("condition_start_date").isNull(), 1).otherwise(0)).alias("null_condition_start_date"),
        F.sum(F.when(
            F.col("condition_end_date").isNotNull() & (F.col("condition_end_date") < F.col("condition_start_date")), 1
        ).otherwise(0)).alias("bad_date_condition_end_date"),
    ).collect()[0]
    return [
        ValidationResult("condition_occurrence", "null_person_id", int(row["null_person_id"])),
        ValidationResult("condition_occurrence", "null_condition_concept_id", int(row["null_condition_concept_id"])),
        ValidationResult("condition_occurrence", "null_condition_start_date", int(row["null_condition_start_date"])),
        ValidationResult("condition_occurrence", "bad_date_condition_end_date", int(row["bad_date_condition_end_date"])),
        ValidationResult("condition_occurrence", "dup_pk", _dup_count(df, "condition_occurrence_id")),
    ]


def validate_drug_exposure(df: DataFrame) -> list[ValidationResult]:
    row = df.agg(
        F.sum(F.when(F.col("person_id").isNull(), 1).otherwise(0)).alias("null_person_id"),
        F.sum(F.when(F.col("drug_concept_id").isNull(), 1).otherwise(0)).alias("null_drug_concept_id"),
        F.sum(F.when(F.col("drug_exposure_start_date").isNull(), 1).otherwise(0)).alias("null_drug_exposure_start_date"),
        F.sum(F.when(F.col("days_supply").isNull(), 1).otherwise(0)).alias("null_days_supply"),
        F.sum(F.when(F.col("quantity").isNull(), 1).otherwise(0)).alias("null_quantity"),
        F.sum(F.when(
            F.col("drug_exposure_end_date").isNotNull() & (F.col("drug_exposure_end_date") < F.col("drug_exposure_start_date")), 1
        ).otherwise(0)).alias("bad_date_drug_exposure_end_date"),
        F.sum(F.when(F.col("days_supply") < 0, 1).otherwise(0)).alias("neg_days_supply"),
        F.sum(F.when(F.col("quantity") < 0, 1).otherwise(0)).alias("neg_quantity"),
    ).collect()[0]
    return [
        ValidationResult("drug_exposure", "null_person_id", int(row["null_person_id"])),
        ValidationResult("drug_exposure", "null_drug_concept_id", int(row["null_drug_concept_id"])),
        ValidationResult("drug_exposure", "null_drug_exposure_start_date", int(row["null_drug_exposure_start_date"])),
        ValidationResult("drug_exposure", "null_days_supply", int(row["null_days_supply"])),
        ValidationResult("drug_exposure", "null_quantity", int(row["null_quantity"])),
        ValidationResult("drug_exposure", "bad_date_drug_exposure_end_date", int(row["bad_date_drug_exposure_end_date"])),
        ValidationResult("drug_exposure", "neg_days_supply", int(row["neg_days_supply"])),
        ValidationResult("drug_exposure", "neg_quantity", int(row["neg_quantity"])),
        ValidationResult("drug_exposure", "dup_pk", _dup_count(df, "drug_exposure_id")),
    ]


def validate_measurement(df: DataFrame, person: DataFrame) -> list[ValidationResult]:
    row = df.agg(
        F.sum(F.when(F.col("person_id").isNull(), 1).otherwise(0)).alias("null_person_id"),
        F.sum(F.when(F.col("measurement_concept_id").isNull(), 1).otherwise(0)).alias("null_measurement_concept_id"),
        F.sum(F.when(F.col("measurement_date").isNull(), 1).otherwise(0)).alias("null_measurement_date"),
        F.sum(F.when(F.col("value_as_number").isNull(), 1).otherwise(0)).alias("null_value_as_number"),
        F.sum(F.when(F.col("value_as_number") < 0, 1).otherwise(0)).alias("neg_value_as_number"),
    ).collect()[0]
    return [
        ValidationResult("measurement", "null_person_id", int(row["null_person_id"])),
        ValidationResult("measurement", "null_measurement_concept_id", int(row["null_measurement_concept_id"])),
        ValidationResult("measurement", "null_measurement_date", int(row["null_measurement_date"])),
        ValidationResult("measurement", "null_value_as_number", int(row["null_value_as_number"])),
        ValidationResult("measurement", "neg_value_as_number", int(row["neg_value_as_number"])),
        ValidationResult("measurement", "orphan_person_id", _orphan_count(df, "person_id", person, "person_id")),
        ValidationResult("measurement", "dup_pk", _dup_count(df, "measurement_id")),
    ]


def validate_note(df: DataFrame, visit: DataFrame) -> list[ValidationResult]:
    row = df.agg(
        F.sum(F.when(F.col("person_id").isNull(), 1).otherwise(0)).alias("null_person_id"),
        F.sum(F.when(F.col("note_date").isNull(), 1).otherwise(0)).alias("null_note_date"),
        F.sum(F.when(F.col("note_text").isNull(), 1).otherwise(0)).alias("null_note_text"),
    ).collect()[0]
    return [
        ValidationResult("note", "null_person_id", int(row["null_person_id"])),
        ValidationResult("note", "null_note_date", int(row["null_note_date"])),
        ValidationResult("note", "null_note_text", int(row["null_note_text"])),
        ValidationResult("note", "orphan_visit_occurrence_id", _orphan_count(df, "visit_occurrence_id", visit, "visit_occurrence_id")),
        ValidationResult("note", "dup_pk", _dup_count(df, "note_id")),
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

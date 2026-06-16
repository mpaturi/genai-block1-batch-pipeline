"""Tests for src/pipeline.py — PipelineValidationError and the hard-gate stage."""

import logging
from datetime import date

import pytest
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, StringType, StructField, StructType
)

from src.pipeline import PipelineValidationError, _log_validation_results, _validate_cleaned
from src.transforms import CleanedTables
from src.validations import ValidationResult

# ---------------------------------------------------------------------------
# Schemas (all nullable=True for injection flexibility)
# ---------------------------------------------------------------------------

PERSON_SCHEMA = StructType([
    StructField("person_id",            IntegerType(), True),
    StructField("gender_concept_id",    IntegerType(), True),
    StructField("year_of_birth",        IntegerType(), True),
    StructField("race_concept_id",      IntegerType(), True),
    StructField("ethnicity_concept_id", IntegerType(), True),
])

VISIT_SCHEMA = StructType([
    StructField("visit_occurrence_id", IntegerType(), True),
    StructField("person_id",           IntegerType(), True),
    StructField("visit_concept_id",    IntegerType(), True),
    StructField("visit_start_date",    DateType(),    True),
    StructField("visit_end_date",      DateType(),    True),
])

CONDITION_SCHEMA = StructType([
    StructField("condition_occurrence_id", IntegerType(), True),
    StructField("person_id",               IntegerType(), True),
    StructField("condition_concept_id",    IntegerType(), True),
    StructField("condition_start_date",    DateType(),    True),
    StructField("condition_end_date",      DateType(),    True),
])

DRUG_SCHEMA = StructType([
    StructField("drug_exposure_id",         IntegerType(), True),
    StructField("person_id",                IntegerType(), True),
    StructField("drug_concept_id",          IntegerType(), True),
    StructField("drug_exposure_start_date", DateType(),    True),
    StructField("drug_exposure_end_date",   DateType(),    True),
    StructField("days_supply",              IntegerType(), True),
    StructField("quantity",                 DoubleType(),  True),
])

MEASUREMENT_SCHEMA = StructType([
    StructField("measurement_id",         IntegerType(), True),
    StructField("person_id",              IntegerType(), True),
    StructField("measurement_concept_id", IntegerType(), True),
    StructField("measurement_date",       DateType(),    True),
    StructField("value_as_number",        DoubleType(),  True),
])

NOTE_SCHEMA = StructType([
    StructField("note_id",             IntegerType(), True),
    StructField("person_id",           IntegerType(), True),
    StructField("note_date",           DateType(),    True),
    StructField("note_text",           StringType(),  True),
    StructField("visit_occurrence_id", IntegerType(), True),
])

D1 = date(2020, 1, 1)
D2 = date(2020, 6, 1)


def _clean_tables(spark):
    """Return a CleanedTables instance with one valid row per table."""
    person = spark.createDataFrame([(1, 8507, 1980, 8516, 38003564)], PERSON_SCHEMA)
    visit  = spark.createDataFrame([(10, 1, 9202, D1, D2)], VISIT_SCHEMA)
    cond   = spark.createDataFrame([(100, 1, 201826, D1, D2)], CONDITION_SCHEMA)
    drug   = spark.createDataFrame([(200, 1, 1503184, D1, D2, 30, 1.0)], DRUG_SCHEMA)
    meas   = spark.createDataFrame([(300, 1, 3004410, D1, 5.5)], MEASUREMENT_SCHEMA)
    note   = spark.createDataFrame([(400, 1, D1, "text", 10)], NOTE_SCHEMA)
    return CleanedTables(person=person, visit=visit, condition=cond,
                         drug=drug, measurement=meas, note=note)


# ---------------------------------------------------------------------------
# PipelineValidationError
# ---------------------------------------------------------------------------

class TestPipelineValidationError:
    def test_is_runtime_error(self):
        assert issubclass(PipelineValidationError, RuntimeError)

    def test_can_be_raised_and_caught(self):
        with pytest.raises(PipelineValidationError, match="violation"):
            raise PipelineValidationError("1 violation")


# ---------------------------------------------------------------------------
# _validate_cleaned
# ---------------------------------------------------------------------------

class TestValidateCleaned:
    def test_passes_silently_with_clean_tables(self, spark):
        tables = _clean_tables(spark)
        _validate_cleaned(tables)  # should not raise

    def test_raises_when_violations_remain(self, spark):
        tables = _clean_tables(spark)
        # Replace person with a dirty row (null gender stays after "cleaning")
        dirty_person = spark.createDataFrame(
            [(1, None, 1980, 8516, 38003564)], PERSON_SCHEMA
        )
        tables = CleanedTables(
            person=dirty_person,
            visit=tables.visit,
            condition=tables.condition,
            drug=tables.drug,
            measurement=tables.measurement,
            note=tables.note,
        )
        with pytest.raises(PipelineValidationError):
            _validate_cleaned(tables)

    def test_error_message_names_the_violation(self, spark):
        tables = _clean_tables(spark)
        dirty_person = spark.createDataFrame(
            [(1, None, 1980, 8516, 38003564)], PERSON_SCHEMA
        )
        tables = CleanedTables(
            person=dirty_person,
            visit=tables.visit,
            condition=tables.condition,
            drug=tables.drug,
            measurement=tables.measurement,
            note=tables.note,
        )
        with pytest.raises(PipelineValidationError, match="null_gender_concept_id"):
            _validate_cleaned(tables)


# ---------------------------------------------------------------------------
# _log_validation_results
# ---------------------------------------------------------------------------

class TestLogValidationResults:
    def test_no_warning_when_all_clean(self, caplog):
        results = [
            ValidationResult("person", "null_gender_concept_id", 0),
            ValidationResult("person", "dup_pk", 0),
        ]
        with caplog.at_level(logging.WARNING, logger="src.pipeline"):
            _log_validation_results(results, "RAW")
        assert not any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_warns_on_violations(self, caplog):
        results = [
            ValidationResult("person", "null_gender_concept_id", 3, [1, 2, 3]),
            ValidationResult("person", "dup_pk", 0),
        ]
        with caplog.at_level(logging.WARNING, logger="src.pipeline"):
            _log_validation_results(results, "RAW")
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) >= 1

    def test_log_message_includes_stage(self, caplog):
        results = [ValidationResult("person", "dup_pk", 2)]
        with caplog.at_level(logging.WARNING, logger="src.pipeline"):
            _log_validation_results(results, "MY_STAGE")
        assert any("MY_STAGE" in r.message for r in caplog.records)

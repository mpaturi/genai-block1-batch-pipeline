"""Tests for src/validations.py — one test per check, per table."""

from datetime import date

import pytest
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, StringType, StructField, StructType
)

from src.validations import (
    validate_all,
    validate_condition_occurrence,
    validate_drug_exposure,
    validate_measurement,
    validate_note,
    validate_person,
    validate_visit_occurrence,
)

# ---------------------------------------------------------------------------
# Minimal schemas (all nullable=True so tests can inject nulls freely)
# ---------------------------------------------------------------------------

PERSON_SCHEMA = StructType([
    StructField("person_id",           IntegerType(), True),
    StructField("gender_concept_id",   IntegerType(), True),
    StructField("year_of_birth",       IntegerType(), True),
    StructField("race_concept_id",     IntegerType(), True),
    StructField("ethnicity_concept_id",IntegerType(), True),
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
    StructField("drug_exposure_id",          IntegerType(), True),
    StructField("person_id",                 IntegerType(), True),
    StructField("drug_concept_id",           IntegerType(), True),
    StructField("drug_exposure_start_date",  DateType(),    True),
    StructField("drug_exposure_end_date",    DateType(),    True),
    StructField("days_supply",               IntegerType(), True),
    StructField("quantity",                  DoubleType(),  True),
])

MEASUREMENT_SCHEMA = StructType([
    StructField("measurement_id",         IntegerType(), True),
    StructField("person_id",              IntegerType(), True),
    StructField("measurement_concept_id", IntegerType(), True),
    StructField("measurement_date",       DateType(),    True),
    StructField("value_as_number",        DoubleType(),  True),
])

NOTE_SCHEMA = StructType([
    StructField("note_id",               IntegerType(), True),
    StructField("person_id",             IntegerType(), True),
    StructField("note_date",             DateType(),    True),
    StructField("note_text",             StringType(),  True),
    StructField("visit_occurrence_id",   IntegerType(), True),
])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

D1 = date(2020, 1, 1)
D2 = date(2020, 6, 1)
D_BEFORE = date(2019, 12, 31)  # earlier than D1


def _result(results, check):
    """Return the single ValidationResult whose check field matches."""
    matches = [r for r in results if r.check == check]
    assert len(matches) == 1, f"Expected 1 result for check={check!r}, got {len(matches)}"
    return matches[0]


# ---------------------------------------------------------------------------
# validate_person
# ---------------------------------------------------------------------------

class TestValidatePerson:
    def _person(self, spark, rows):
        return spark.createDataFrame(rows, PERSON_SCHEMA)

    def _clean_row(self):
        return (1, 8507, 1980, 8516, 38003564)

    def test_clean_data_no_violations(self, spark):
        df = self._person(spark, [self._clean_row()])
        results = validate_person(df)
        assert all(r.count == 0 for r in results)

    def test_null_gender_concept_id(self, spark):
        df = self._person(spark, [(1, None, 1980, 8516, 38003564)])
        r = _result(validate_person(df), "null_gender_concept_id")
        assert r.count == 1
        assert 1 in r.bad_ids

    def test_null_year_of_birth(self, spark):
        df = self._person(spark, [(1, 8507, None, 8516, 38003564)])
        r = _result(validate_person(df), "null_year_of_birth")
        assert r.count == 1

    def test_null_race_concept_id(self, spark):
        df = self._person(spark, [(1, 8507, 1980, None, 38003564)])
        r = _result(validate_person(df), "null_race_concept_id")
        assert r.count == 1

    def test_null_ethnicity_concept_id(self, spark):
        df = self._person(spark, [(1, 8507, 1980, 8516, None)])
        r = _result(validate_person(df), "null_ethnicity_concept_id")
        assert r.count == 1

    def test_dup_pk(self, spark):
        df = self._person(spark, [self._clean_row(), self._clean_row()])
        r = _result(validate_person(df), "dup_pk")
        assert r.count == 1  # one extra row

    def test_no_dup_when_unique(self, spark):
        df = self._person(spark, [self._clean_row(), (2, 8507, 1985, 8516, 38003564)])
        r = _result(validate_person(df), "dup_pk")
        assert r.count == 0


# ---------------------------------------------------------------------------
# validate_visit_occurrence
# ---------------------------------------------------------------------------

class TestValidateVisitOccurrence:
    def _visit(self, spark, rows):
        return spark.createDataFrame(rows, VISIT_SCHEMA)

    def _person(self, spark):
        return spark.createDataFrame([(1, 8507, 1980, 8516, 38003564)], PERSON_SCHEMA)

    def _clean_row(self):
        return (10, 1, 9202, D1, D2)

    def test_clean_data_no_violations(self, spark):
        df = self._visit(spark, [self._clean_row()])
        results = validate_visit_occurrence(df, self._person(spark))
        assert all(r.count == 0 for r in results)

    def test_null_person_id(self, spark):
        df = self._visit(spark, [(10, None, 9202, D1, D2)])
        r = _result(validate_visit_occurrence(df, self._person(spark)), "null_person_id")
        assert r.count == 1

    def test_null_visit_concept_id(self, spark):
        df = self._visit(spark, [(10, 1, None, D1, D2)])
        r = _result(validate_visit_occurrence(df, self._person(spark)), "null_visit_concept_id")
        assert r.count == 1

    def test_null_visit_start_date(self, spark):
        df = self._visit(spark, [(10, 1, 9202, None, D2)])
        r = _result(validate_visit_occurrence(df, self._person(spark)), "null_visit_start_date")
        assert r.count == 1

    def test_null_visit_end_date(self, spark):
        df = self._visit(spark, [(10, 1, 9202, D1, None)])
        r = _result(validate_visit_occurrence(df, self._person(spark)), "null_visit_end_date")
        assert r.count == 1

    def test_bad_date_end_before_start(self, spark):
        df = self._visit(spark, [(10, 1, 9202, D2, D1)])  # end < start
        r = _result(validate_visit_occurrence(df, self._person(spark)), "bad_date_visit_end_date")
        assert r.count == 1

    def test_orphan_person_id(self, spark):
        df = self._visit(spark, [(10, 99, 9202, D1, D2)])  # person 99 not in parent
        r = _result(validate_visit_occurrence(df, self._person(spark)), "orphan_person_id")
        assert r.count == 1

    def test_dup_pk(self, spark):
        df = self._visit(spark, [self._clean_row(), self._clean_row()])
        r = _result(validate_visit_occurrence(df, self._person(spark)), "dup_pk")
        assert r.count == 1


# ---------------------------------------------------------------------------
# validate_condition_occurrence
# ---------------------------------------------------------------------------

class TestValidateConditionOccurrence:
    def _cond(self, spark, rows):
        return spark.createDataFrame(rows, CONDITION_SCHEMA)

    def _clean_row(self):
        return (100, 1, 201826, D1, D2)

    def test_clean_data_no_violations(self, spark):
        df = self._cond(spark, [self._clean_row()])
        assert all(r.count == 0 for r in validate_condition_occurrence(df))

    def test_null_person_id(self, spark):
        df = self._cond(spark, [(100, None, 201826, D1, D2)])
        r = _result(validate_condition_occurrence(df), "null_person_id")
        assert r.count == 1

    def test_null_condition_concept_id(self, spark):
        df = self._cond(spark, [(100, 1, None, D1, D2)])
        r = _result(validate_condition_occurrence(df), "null_condition_concept_id")
        assert r.count == 1

    def test_null_condition_start_date(self, spark):
        df = self._cond(spark, [(100, 1, 201826, None, D2)])
        r = _result(validate_condition_occurrence(df), "null_condition_start_date")
        assert r.count == 1

    def test_bad_date_end_before_start(self, spark):
        df = self._cond(spark, [(100, 1, 201826, D2, D1)])
        r = _result(validate_condition_occurrence(df), "bad_date_condition_end_date")
        assert r.count == 1

    def test_null_end_date_is_allowed(self, spark):
        df = self._cond(spark, [(100, 1, 201826, D1, None)])
        r = _result(validate_condition_occurrence(df), "bad_date_condition_end_date")
        assert r.count == 0

    def test_dup_pk(self, spark):
        df = self._cond(spark, [self._clean_row(), self._clean_row()])
        r = _result(validate_condition_occurrence(df), "dup_pk")
        assert r.count == 1


# ---------------------------------------------------------------------------
# validate_drug_exposure
# ---------------------------------------------------------------------------

class TestValidateDrugExposure:
    def _drug(self, spark, rows):
        return spark.createDataFrame(rows, DRUG_SCHEMA)

    def _clean_row(self):
        return (200, 1, 1503184, D1, D2, 30, 1.0)

    def test_clean_data_no_violations(self, spark):
        df = self._drug(spark, [self._clean_row()])
        assert all(r.count == 0 for r in validate_drug_exposure(df))

    def test_null_person_id(self, spark):
        df = self._drug(spark, [(200, None, 1503184, D1, D2, 30, 1.0)])
        r = _result(validate_drug_exposure(df), "null_person_id")
        assert r.count == 1

    def test_null_drug_concept_id(self, spark):
        df = self._drug(spark, [(200, 1, None, D1, D2, 30, 1.0)])
        r = _result(validate_drug_exposure(df), "null_drug_concept_id")
        assert r.count == 1

    def test_null_drug_exposure_start_date(self, spark):
        df = self._drug(spark, [(200, 1, 1503184, None, D2, 30, 1.0)])
        r = _result(validate_drug_exposure(df), "null_drug_exposure_start_date")
        assert r.count == 1

    def test_null_days_supply(self, spark):
        df = self._drug(spark, [(200, 1, 1503184, D1, D2, None, 1.0)])
        r = _result(validate_drug_exposure(df), "null_days_supply")
        assert r.count == 1

    def test_null_quantity(self, spark):
        df = self._drug(spark, [(200, 1, 1503184, D1, D2, 30, None)])
        r = _result(validate_drug_exposure(df), "null_quantity")
        assert r.count == 1

    def test_bad_date_end_before_start(self, spark):
        df = self._drug(spark, [(200, 1, 1503184, D2, D1, 30, 1.0)])
        r = _result(validate_drug_exposure(df), "bad_date_drug_exposure_end_date")
        assert r.count == 1

    def test_negative_days_supply(self, spark):
        df = self._drug(spark, [(200, 1, 1503184, D1, D2, -1, 1.0)])
        r = _result(validate_drug_exposure(df), "neg_days_supply")
        assert r.count == 1

    def test_negative_quantity(self, spark):
        df = self._drug(spark, [(200, 1, 1503184, D1, D2, 30, -0.5)])
        r = _result(validate_drug_exposure(df), "neg_quantity")
        assert r.count == 1

    def test_zero_days_supply_is_not_negative(self, spark):
        df = self._drug(spark, [(200, 1, 1503184, D1, D2, 0, 1.0)])
        r = _result(validate_drug_exposure(df), "neg_days_supply")
        assert r.count == 0

    def test_dup_pk(self, spark):
        df = self._drug(spark, [self._clean_row(), self._clean_row()])
        r = _result(validate_drug_exposure(df), "dup_pk")
        assert r.count == 1


# ---------------------------------------------------------------------------
# validate_measurement
# ---------------------------------------------------------------------------

class TestValidateMeasurement:
    def _meas(self, spark, rows):
        return spark.createDataFrame(rows, MEASUREMENT_SCHEMA)

    def _person(self, spark):
        return spark.createDataFrame([(1, 8507, 1980, 8516, 38003564)], PERSON_SCHEMA)

    def _clean_row(self):
        return (300, 1, 3004410, D1, 5.5)

    def test_clean_data_no_violations(self, spark):
        df = self._meas(spark, [self._clean_row()])
        assert all(r.count == 0 for r in validate_measurement(df, self._person(spark)))

    def test_null_person_id(self, spark):
        df = self._meas(spark, [(300, None, 3004410, D1, 5.5)])
        r = _result(validate_measurement(df, self._person(spark)), "null_person_id")
        assert r.count == 1

    def test_null_measurement_concept_id(self, spark):
        df = self._meas(spark, [(300, 1, None, D1, 5.5)])
        r = _result(validate_measurement(df, self._person(spark)), "null_measurement_concept_id")
        assert r.count == 1

    def test_null_measurement_date(self, spark):
        df = self._meas(spark, [(300, 1, 3004410, None, 5.5)])
        r = _result(validate_measurement(df, self._person(spark)), "null_measurement_date")
        assert r.count == 1

    def test_null_value_as_number(self, spark):
        df = self._meas(spark, [(300, 1, 3004410, D1, None)])
        r = _result(validate_measurement(df, self._person(spark)), "null_value_as_number")
        assert r.count == 1

    def test_negative_value_as_number(self, spark):
        df = self._meas(spark, [(300, 1, 3004410, D1, -1.0)])
        r = _result(validate_measurement(df, self._person(spark)), "neg_value_as_number")
        assert r.count == 1

    def test_orphan_person_id(self, spark):
        df = self._meas(spark, [(300, 99, 3004410, D1, 5.5)])  # person 99 not in parent
        r = _result(validate_measurement(df, self._person(spark)), "orphan_person_id")
        assert r.count == 1

    def test_dup_pk(self, spark):
        df = self._meas(spark, [self._clean_row(), self._clean_row()])
        r = _result(validate_measurement(df, self._person(spark)), "dup_pk")
        assert r.count == 1


# ---------------------------------------------------------------------------
# validate_note
# ---------------------------------------------------------------------------

class TestValidateNote:
    def _note(self, spark, rows):
        return spark.createDataFrame(rows, NOTE_SCHEMA)

    def _visit(self, spark):
        return spark.createDataFrame([(10, 1, 9202, D1, D2)], VISIT_SCHEMA)

    def _clean_row(self):
        return (400, 1, D1, "note text", 10)

    def test_clean_data_no_violations(self, spark):
        df = self._note(spark, [self._clean_row()])
        assert all(r.count == 0 for r in validate_note(df, self._visit(spark)))

    def test_null_person_id(self, spark):
        df = self._note(spark, [(400, None, D1, "text", 10)])
        r = _result(validate_note(df, self._visit(spark)), "null_person_id")
        assert r.count == 1

    def test_null_note_date(self, spark):
        df = self._note(spark, [(400, 1, None, "text", 10)])
        r = _result(validate_note(df, self._visit(spark)), "null_note_date")
        assert r.count == 1

    def test_null_note_text(self, spark):
        df = self._note(spark, [(400, 1, D1, None, 10)])
        r = _result(validate_note(df, self._visit(spark)), "null_note_text")
        assert r.count == 1

    def test_orphan_visit_occurrence_id(self, spark):
        df = self._note(spark, [(400, 1, D1, "text", 99)])  # visit 99 not in parent
        r = _result(validate_note(df, self._visit(spark)), "orphan_visit_occurrence_id")
        assert r.count == 1

    def test_null_visit_occurrence_id_is_allowed(self, spark):
        df = self._note(spark, [(400, 1, D1, "text", None)])
        r = _result(validate_note(df, self._visit(spark)), "orphan_visit_occurrence_id")
        assert r.count == 0

    def test_dup_pk(self, spark):
        df = self._note(spark, [self._clean_row(), self._clean_row()])
        r = _result(validate_note(df, self._visit(spark)), "dup_pk")
        assert r.count == 1


# ---------------------------------------------------------------------------
# validate_all
# ---------------------------------------------------------------------------

class TestValidateAll:
    def test_returns_combined_results(self, spark):
        person = spark.createDataFrame([(1, 8507, 1980, 8516, 38003564)], PERSON_SCHEMA)
        visit  = spark.createDataFrame([(10, 1, 9202, D1, D2)], VISIT_SCHEMA)
        cond   = spark.createDataFrame([(100, 1, 201826, D1, D2)], CONDITION_SCHEMA)
        drug   = spark.createDataFrame([(200, 1, 1503184, D1, D2, 30, 1.0)], DRUG_SCHEMA)
        meas   = spark.createDataFrame([(300, 1, 3004410, D1, 5.5)], MEASUREMENT_SCHEMA)
        note   = spark.createDataFrame([(400, 1, D1, "text", 10)], NOTE_SCHEMA)

        results = validate_all(person, visit, cond, drug, meas, note)
        assert len(results) > 0
        assert all(r.count == 0 for r in results)

    def test_surfaces_violations_from_all_tables(self, spark):
        # Inject one violation in each of two tables
        person = spark.createDataFrame([(1, None, 1980, 8516, 38003564)], PERSON_SCHEMA)  # null gender
        visit  = spark.createDataFrame([(10, 1, 9202, D2, D1)], VISIT_SCHEMA)  # bad date
        cond   = spark.createDataFrame([(100, 1, 201826, D1, D2)], CONDITION_SCHEMA)
        drug   = spark.createDataFrame([(200, 1, 1503184, D1, D2, 30, 1.0)], DRUG_SCHEMA)
        meas   = spark.createDataFrame([(300, 1, 3004410, D1, 5.5)], MEASUREMENT_SCHEMA)
        note   = spark.createDataFrame([(400, 1, D1, "text", 10)], NOTE_SCHEMA)

        results = validate_all(person, visit, cond, drug, meas, note)
        violations = [r for r in results if r.count > 0]
        checks = {r.check for r in violations}
        assert "null_gender_concept_id" in checks
        assert "bad_date_visit_end_date" in checks

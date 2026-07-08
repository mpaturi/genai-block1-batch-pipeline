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
    """One combined dirty dataset covers 5 of the 6 checks in a single
    validate_person() call; the clean-data and no-false-positive-dup cases
    stay as separate, minimal fixtures since they test distinct datasets
    that can't be merged without losing what they specifically prove."""

    @pytest.fixture(scope="class")
    def dirty_results(self, spark):
        rows = [
            (1, 8507, 1980, 8516, 38003564),   # baseline (not itself asserted on)
            (2, None, 1980, 8516, 38003564),   # null gender_concept_id
            (3, 8507, None, 8516, 38003564),   # null year_of_birth
            (4, 8507, 1980, None, 38003564),   # null race_concept_id
            (5, 8507, 1980, 8516, None),       # null ethnicity_concept_id
            (6, 8507, 1980, 8516, 38003564),   # duplicate pair -> dup_pk
            (6, 8507, 1980, 8516, 38003564),
        ]
        df = spark.createDataFrame(rows, PERSON_SCHEMA)
        return validate_person(df)

    def test_null_gender_concept_id(self, dirty_results):
        assert _result(dirty_results, "null_gender_concept_id").count == 1

    def test_null_year_of_birth(self, dirty_results):
        assert _result(dirty_results, "null_year_of_birth").count == 1

    def test_null_race_concept_id(self, dirty_results):
        assert _result(dirty_results, "null_race_concept_id").count == 1

    def test_null_ethnicity_concept_id(self, dirty_results):
        assert _result(dirty_results, "null_ethnicity_concept_id").count == 1

    def test_dup_pk(self, dirty_results):
        assert _result(dirty_results, "dup_pk").count == 1

    def test_clean_data_no_violations(self, spark):
        df = spark.createDataFrame([(1, 8507, 1980, 8516, 38003564)], PERSON_SCHEMA)
        assert all(r.count == 0 for r in validate_person(df))

    def test_no_dup_when_unique(self, spark):
        df = spark.createDataFrame([
            (1, 8507, 1980, 8516, 38003564),
            (2, 8507, 1985, 8516, 38003564),
        ], PERSON_SCHEMA)
        assert _result(validate_person(df), "dup_pk").count == 0


# ---------------------------------------------------------------------------
# validate_visit_occurrence
# ---------------------------------------------------------------------------

class TestValidateVisitOccurrence:
    """One combined dirty dataset covers 6 of the 7 checks in a single
    validate_visit_occurrence() call; clean-data stays separate since it's
    a distinct all-valid dataset."""

    @pytest.fixture(scope="class")
    def dirty_results(self, spark):
        person = spark.createDataFrame([(1, 8507, 1980, 8516, 38003564)], PERSON_SCHEMA)
        rows = [
            (10, 1, 9202, D1, D2),      # baseline (not itself asserted on)
            (11, None, 9202, D1, D2),   # null_person_id
            (12, 1, None, D1, D2),      # null_visit_concept_id
            (13, 1, 9202, None, D2),    # null_visit_start_date
            (14, 1, 9202, D1, None),    # null_visit_end_date
            (15, 1, 9202, D2, D1),      # bad_date_visit_end_date (end < start)
            (16, 99, 9202, D1, D2),     # orphan_person_id (person 99 not in parent)
            (17, 1, 9202, D1, D2),      # dup_pk pair
            (17, 1, 9202, D1, D2),
        ]
        df = spark.createDataFrame(rows, VISIT_SCHEMA)
        return validate_visit_occurrence(df, person)

    def test_null_person_id(self, dirty_results):
        assert _result(dirty_results, "null_person_id").count == 1

    def test_null_visit_concept_id(self, dirty_results):
        assert _result(dirty_results, "null_visit_concept_id").count == 1

    def test_null_visit_start_date(self, dirty_results):
        assert _result(dirty_results, "null_visit_start_date").count == 1

    def test_null_visit_end_date(self, dirty_results):
        assert _result(dirty_results, "null_visit_end_date").count == 1

    def test_bad_date_end_before_start(self, dirty_results):
        assert _result(dirty_results, "bad_date_visit_end_date").count == 1

    def test_orphan_person_id(self, dirty_results):
        assert _result(dirty_results, "orphan_person_id").count == 1

    def test_dup_pk(self, dirty_results):
        assert _result(dirty_results, "dup_pk").count == 1

    def test_clean_data_no_violations(self, spark):
        person = spark.createDataFrame([(1, 8507, 1980, 8516, 38003564)], PERSON_SCHEMA)
        df = spark.createDataFrame([(10, 1, 9202, D1, D2)], VISIT_SCHEMA)
        assert all(r.count == 0 for r in validate_visit_occurrence(df, person))


# ---------------------------------------------------------------------------
# validate_condition_occurrence
# ---------------------------------------------------------------------------

class TestValidateConditionOccurrence:
    """Combined dirty dataset covers 5 of 7 checks in one call; clean-data and
    the null-end-date-allowed case stay separate since each needs its own
    isolated dataset to precisely prove its (often negative) claim."""

    @pytest.fixture(scope="class")
    def dirty_results(self, spark):
        rows = [
            (100, 1, 201826, D1, D2),     # baseline (not itself asserted on)
            (101, None, 201826, D1, D2),  # null_person_id
            (102, 1, None, D1, D2),       # null_condition_concept_id
            (103, 1, 201826, None, D2),   # null_condition_start_date
            (104, 1, 201826, D2, D1),     # bad_date_condition_end_date (end<start)
            (105, 1, 201826, D1, D2),     # dup_pk pair
            (105, 1, 201826, D1, D2),
        ]
        df = spark.createDataFrame(rows, CONDITION_SCHEMA)
        return validate_condition_occurrence(df)

    def test_null_person_id(self, dirty_results):
        assert _result(dirty_results, "null_person_id").count == 1

    def test_null_condition_concept_id(self, dirty_results):
        assert _result(dirty_results, "null_condition_concept_id").count == 1

    def test_null_condition_start_date(self, dirty_results):
        assert _result(dirty_results, "null_condition_start_date").count == 1

    def test_bad_date_end_before_start(self, dirty_results):
        assert _result(dirty_results, "bad_date_condition_end_date").count == 1

    def test_dup_pk(self, dirty_results):
        assert _result(dirty_results, "dup_pk").count == 1

    def test_null_end_date_is_allowed(self, spark):
        df = spark.createDataFrame([(100, 1, 201826, D1, None)], CONDITION_SCHEMA)
        r = _result(validate_condition_occurrence(df), "bad_date_condition_end_date")
        assert r.count == 0

    def test_clean_data_no_violations(self, spark):
        df = spark.createDataFrame([(100, 1, 201826, D1, D2)], CONDITION_SCHEMA)
        assert all(r.count == 0 for r in validate_condition_occurrence(df))


# ---------------------------------------------------------------------------
# validate_drug_exposure
# ---------------------------------------------------------------------------

class TestValidateDrugExposure:
    """Combined dirty dataset covers 9 of 11 checks in one call; clean-data and
    the zero-is-not-negative case stay separate to avoid two tests silently
    asserting the exact same aggregate value under different names."""

    @pytest.fixture(scope="class")
    def dirty_results(self, spark):
        rows = [
            (200, 1, 1503184, D1, D2, 30, 1.0),      # baseline (not itself asserted on)
            (201, None, 1503184, D1, D2, 30, 1.0),   # null_person_id
            (202, 1, None, D1, D2, 30, 1.0),          # null_drug_concept_id
            (203, 1, 1503184, None, D2, 30, 1.0),     # null_drug_exposure_start_date
            (204, 1, 1503184, D1, D2, None, 1.0),     # null_days_supply
            (205, 1, 1503184, D1, D2, 30, None),      # null_quantity
            (206, 1, 1503184, D2, D1, 30, 1.0),       # bad_date_drug_exposure_end_date
            (207, 1, 1503184, D1, D2, -1, 1.0),       # neg_days_supply
            (208, 1, 1503184, D1, D2, 30, -0.5),      # neg_quantity
            (209, 1, 1503184, D1, D2, 30, 1.0),       # dup_pk pair
            (209, 1, 1503184, D1, D2, 30, 1.0),
        ]
        df = spark.createDataFrame(rows, DRUG_SCHEMA)
        return validate_drug_exposure(df)

    def test_null_person_id(self, dirty_results):
        assert _result(dirty_results, "null_person_id").count == 1

    def test_null_drug_concept_id(self, dirty_results):
        assert _result(dirty_results, "null_drug_concept_id").count == 1

    def test_null_drug_exposure_start_date(self, dirty_results):
        assert _result(dirty_results, "null_drug_exposure_start_date").count == 1

    def test_null_days_supply(self, dirty_results):
        assert _result(dirty_results, "null_days_supply").count == 1

    def test_null_quantity(self, dirty_results):
        assert _result(dirty_results, "null_quantity").count == 1

    def test_bad_date_end_before_start(self, dirty_results):
        assert _result(dirty_results, "bad_date_drug_exposure_end_date").count == 1

    def test_negative_days_supply(self, dirty_results):
        assert _result(dirty_results, "neg_days_supply").count == 1

    def test_negative_quantity(self, dirty_results):
        assert _result(dirty_results, "neg_quantity").count == 1

    def test_dup_pk(self, dirty_results):
        assert _result(dirty_results, "dup_pk").count == 1

    def test_zero_days_supply_is_not_negative(self, spark):
        df = spark.createDataFrame([(200, 1, 1503184, D1, D2, 0, 1.0)], DRUG_SCHEMA)
        r = _result(validate_drug_exposure(df), "neg_days_supply")
        assert r.count == 0

    def test_clean_data_no_violations(self, spark):
        df = spark.createDataFrame([(200, 1, 1503184, D1, D2, 30, 1.0)], DRUG_SCHEMA)
        assert all(r.count == 0 for r in validate_drug_exposure(df))


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
    @pytest.fixture(scope="class")
    def clean_tables(self, spark):
        return {
            "person": spark.createDataFrame([(1, 8507, 1980, 8516, 38003564)], PERSON_SCHEMA),
            "visit":  spark.createDataFrame([(10, 1, 9202, D1, D2)], VISIT_SCHEMA),
            "cond":   spark.createDataFrame([(100, 1, 201826, D1, D2)], CONDITION_SCHEMA),
            "drug":   spark.createDataFrame([(200, 1, 1503184, D1, D2, 30, 1.0)], DRUG_SCHEMA),
            "meas":   spark.createDataFrame([(300, 1, 3004410, D1, 5.5)], MEASUREMENT_SCHEMA),
            "note":   spark.createDataFrame([(400, 1, D1, "text", 10)], NOTE_SCHEMA),
        }

    @pytest.fixture(scope="class")
    def clean_results(self, clean_tables):
        t = clean_tables
        return validate_all(t["person"], t["visit"], t["cond"], t["drug"], t["meas"], t["note"])

    def test_returns_combined_results(self, clean_results):
        assert len(clean_results) > 0
        assert all(r.count == 0 for r in clean_results)

    def test_surfaces_violations_from_all_tables(self, spark, clean_tables):
        t = clean_tables
        dirty_person = spark.createDataFrame([(1, None, 1980, 8516, 38003564)], PERSON_SCHEMA)
        dirty_visit  = spark.createDataFrame([(10, 1, 9202, D2, D1)], VISIT_SCHEMA)
        results = validate_all(dirty_person, dirty_visit, t["cond"], t["drug"], t["meas"], t["note"])
        violations = [r for r in results if r.count > 0]
        checks = {r.check for r in violations}
        assert "null_gender_concept_id" in checks
        assert "bad_date_visit_end_date" in checks

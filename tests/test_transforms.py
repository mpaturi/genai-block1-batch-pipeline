"""Tests for src/transforms.py — cleaning functions and build_analytic_person."""

from datetime import date

import pytest
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, StringType, StructField, StructType
)

from src.concepts import (
    CONDITION_DIABETES,
    CONDITION_HYPERTENSION,
    MEASUREMENT_HBA1C,
    MEASUREMENT_SBP,
    VISIT_ER,
    VISIT_INPATIENT,
    VISIT_OUTPATIENT,
)
from src.transforms import (
    build_analytic_person,
    clean_all,
    clean_condition_occurrence,
    clean_drug_exposure,
    clean_measurement,
    clean_note,
    clean_person,
    clean_visit_occurrence,
)

# ---------------------------------------------------------------------------
# Shared schemas (all nullable=True for injection flexibility)
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
    StructField("note_id",             IntegerType(), True),
    StructField("person_id",           IntegerType(), True),
    StructField("note_date",           DateType(),    True),
    StructField("note_text",           StringType(),  True),
    StructField("visit_occurrence_id", IntegerType(), True),
])

D1 = date(2020, 1, 1)
D2 = date(2020, 6, 1)


# ---------------------------------------------------------------------------
# clean_person
# ---------------------------------------------------------------------------

class TestCleanPerson:
    def _df(self, spark, rows):
        return spark.createDataFrame(rows, PERSON_SCHEMA)

    def test_keeps_clean_rows(self, spark):
        df = self._df(spark, [(1, 8507, 1980, 8516, 38003564)])
        assert clean_person(df).count() == 1

    def test_drops_null_required_field(self, spark):
        df = self._df(spark, [
            (1, 8507, 1980, 8516, 38003564),
            (2, None, 1980, 8516, 38003564),  # null gender
        ])
        assert clean_person(df).count() == 1

    def test_drops_duplicates(self, spark):
        row = (1, 8507, 1980, 8516, 38003564)
        df = self._df(spark, [row, row])
        assert clean_person(df).count() == 1

    def test_drops_null_year_of_birth(self, spark):
        df = self._df(spark, [(1, 8507, None, 8516, 38003564)])
        assert clean_person(df).count() == 0

    def test_drops_null_race_concept_id(self, spark):
        df = self._df(spark, [(1, 8507, 1980, None, 38003564)])
        assert clean_person(df).count() == 0

    def test_drops_null_ethnicity_concept_id(self, spark):
        df = self._df(spark, [(1, 8507, 1980, 8516, None)])
        assert clean_person(df).count() == 0


# ---------------------------------------------------------------------------
# clean_visit_occurrence
# ---------------------------------------------------------------------------

class TestCleanVisitOccurrence:
    def _visit(self, spark, rows):
        return spark.createDataFrame(rows, VISIT_SCHEMA)

    def _person(self, spark):
        return spark.createDataFrame([(1, 8507, 1980, 8516, 38003564)], PERSON_SCHEMA)

    def test_keeps_clean_rows(self, spark):
        df = self._visit(spark, [(10, 1, 9202, D1, D2)])
        assert clean_visit_occurrence(df, self._person(spark)).count() == 1

    def test_drops_null_required_field(self, spark):
        df = self._visit(spark, [(10, None, 9202, D1, D2)])
        assert clean_visit_occurrence(df, self._person(spark)).count() == 0

    def test_drops_bad_dates(self, spark):
        df = self._visit(spark, [(10, 1, 9202, D2, D1)])  # end < start
        assert clean_visit_occurrence(df, self._person(spark)).count() == 0

    def test_keeps_row_when_end_equals_start(self, spark):
        df = self._visit(spark, [(10, 1, 9202, D1, D1)])
        assert clean_visit_occurrence(df, self._person(spark)).count() == 1

    def test_drops_orphan_person_id(self, spark):
        df = self._visit(spark, [(10, 99, 9202, D1, D2)])  # person 99 absent
        assert clean_visit_occurrence(df, self._person(spark)).count() == 0

    def test_drops_duplicates(self, spark):
        row = (10, 1, 9202, D1, D2)
        df = self._visit(spark, [row, row])
        assert clean_visit_occurrence(df, self._person(spark)).count() == 1


# ---------------------------------------------------------------------------
# clean_condition_occurrence
# ---------------------------------------------------------------------------

class TestCleanConditionOccurrence:
    def _df(self, spark, rows):
        return spark.createDataFrame(rows, CONDITION_SCHEMA)

    def test_keeps_clean_rows(self, spark):
        df = self._df(spark, [(100, 1, 201826, D1, D2)])
        assert clean_condition_occurrence(df).count() == 1

    def test_drops_null_required_field(self, spark):
        df = self._df(spark, [(100, None, 201826, D1, D2)])
        assert clean_condition_occurrence(df).count() == 0

    def test_drops_bad_dates(self, spark):
        df = self._df(spark, [(100, 1, 201826, D2, D1)])
        assert clean_condition_occurrence(df).count() == 0

    def test_keeps_null_end_date(self, spark):
        df = self._df(spark, [(100, 1, 201826, D1, None)])
        assert clean_condition_occurrence(df).count() == 1

    def test_drops_duplicates(self, spark):
        row = (100, 1, 201826, D1, D2)
        df = self._df(spark, [row, row])
        assert clean_condition_occurrence(df).count() == 1


# ---------------------------------------------------------------------------
# clean_drug_exposure
# ---------------------------------------------------------------------------

class TestCleanDrugExposure:
    def _df(self, spark, rows):
        return spark.createDataFrame(rows, DRUG_SCHEMA)

    def test_keeps_clean_rows(self, spark):
        df = self._df(spark, [(200, 1, 1503184, D1, D2, 30, 1.0)])
        assert clean_drug_exposure(df).count() == 1

    def test_drops_null_required_field(self, spark):
        df = self._df(spark, [(200, None, 1503184, D1, D2, 30, 1.0)])
        assert clean_drug_exposure(df).count() == 0

    def test_drops_bad_dates(self, spark):
        df = self._df(spark, [(200, 1, 1503184, D2, D1, 30, 1.0)])
        assert clean_drug_exposure(df).count() == 0

    def test_drops_negative_days_supply(self, spark):
        df = self._df(spark, [(200, 1, 1503184, D1, D2, -1, 1.0)])
        assert clean_drug_exposure(df).count() == 0

    def test_drops_negative_quantity(self, spark):
        df = self._df(spark, [(200, 1, 1503184, D1, D2, 30, -0.5)])
        assert clean_drug_exposure(df).count() == 0

    def test_keeps_zero_days_supply(self, spark):
        df = self._df(spark, [(200, 1, 1503184, D1, D2, 0, 1.0)])
        assert clean_drug_exposure(df).count() == 1

    def test_drops_duplicates(self, spark):
        row = (200, 1, 1503184, D1, D2, 30, 1.0)
        df = self._df(spark, [row, row])
        assert clean_drug_exposure(df).count() == 1


# ---------------------------------------------------------------------------
# clean_measurement
# ---------------------------------------------------------------------------

class TestCleanMeasurement:
    def _meas(self, spark, rows):
        return spark.createDataFrame(rows, MEASUREMENT_SCHEMA)

    def _person(self, spark):
        return spark.createDataFrame([(1, 8507, 1980, 8516, 38003564)], PERSON_SCHEMA)

    def test_keeps_clean_rows(self, spark):
        df = self._meas(spark, [(300, 1, 3004410, D1, 5.5)])
        assert clean_measurement(df, self._person(spark)).count() == 1

    def test_drops_null_required_field(self, spark):
        df = self._meas(spark, [(300, None, 3004410, D1, 5.5)])
        assert clean_measurement(df, self._person(spark)).count() == 0

    def test_drops_negative_value(self, spark):
        df = self._meas(spark, [(300, 1, 3004410, D1, -1.0)])
        assert clean_measurement(df, self._person(spark)).count() == 0

    def test_drops_orphan_person_id(self, spark):
        df = self._meas(spark, [(300, 99, 3004410, D1, 5.5)])
        assert clean_measurement(df, self._person(spark)).count() == 0

    def test_drops_duplicates(self, spark):
        row = (300, 1, 3004410, D1, 5.5)
        df = self._meas(spark, [row, row])
        assert clean_measurement(df, self._person(spark)).count() == 1


# ---------------------------------------------------------------------------
# clean_note
# ---------------------------------------------------------------------------

class TestCleanNote:
    def _note(self, spark, rows):
        return spark.createDataFrame(rows, NOTE_SCHEMA)

    def _visit(self, spark):
        return spark.createDataFrame([(10, 1, 9202, D1, D2)], VISIT_SCHEMA)

    def test_keeps_clean_rows(self, spark):
        df = self._note(spark, [(400, 1, D1, "text", 10)])
        assert clean_note(df, self._visit(spark)).count() == 1

    def test_drops_null_required_field(self, spark):
        df = self._note(spark, [(400, 1, D1, None, 10)])
        assert clean_note(df, self._visit(spark)).count() == 0

    def test_drops_orphan_visit(self, spark):
        df = self._note(spark, [(400, 1, D1, "text", 99)])  # visit 99 absent
        assert clean_note(df, self._visit(spark)).count() == 0

    def test_keeps_null_visit_occurrence_id(self, spark):
        df = self._note(spark, [(400, 1, D1, "text", None)])
        assert clean_note(df, self._visit(spark)).count() == 1

    def test_drops_duplicates(self, spark):
        row = (400, 1, D1, "text", 10)
        df = self._note(spark, [row, row])
        assert clean_note(df, self._visit(spark)).count() == 1


# ---------------------------------------------------------------------------
# clean_all
# ---------------------------------------------------------------------------

class TestCleanAll:
    def test_returns_correct_before_after_counts(self, spark):
        person = spark.createDataFrame([
            (1, 8507, 1980, 8516, 38003564),
            (2, None, 1990, 8516, 38003564),  # dirty — null gender
        ], PERSON_SCHEMA)
        visit = spark.createDataFrame([(10, 1, 9202, D1, D2)], VISIT_SCHEMA)
        cond  = spark.createDataFrame([(100, 1, 201826, D1, D2)], CONDITION_SCHEMA)
        drug  = spark.createDataFrame([(200, 1, 1503184, D1, D2, 30, 1.0)], DRUG_SCHEMA)
        meas  = spark.createDataFrame([(300, 1, 3004410, D1, 5.5)], MEASUREMENT_SCHEMA)
        note  = spark.createDataFrame([(400, 1, D1, "text", 10)], NOTE_SCHEMA)

        tables, metrics = clean_all(person, visit, cond, drug, meas, note)

        assert metrics.before["person"] == 2
        assert metrics.after["person"] == 1
        assert tables.person.count() == 1


# ---------------------------------------------------------------------------
# build_analytic_person
# ---------------------------------------------------------------------------

class TestBuildAnalyticPerson:
    @pytest.fixture(scope="class")
    def result(self, spark):
        """Build analytic_person once with two persons covering all test scenarios.

        Person 1 (id=1, year=1980): has visits, conditions, drugs, measurements.
        Person 2 (id=2, year=1983): no clinical data — tests zero/null/false defaults.
        """
        person = spark.createDataFrame([
            (1, 8507, 1980, 8516, 38003564),
            (2, 8532, 1983, 8516, 38003564),
        ], PERSON_SCHEMA)
        visit = spark.createDataFrame([
            (10, 1, VISIT_OUTPATIENT, D1, D2),
            (11, 1, VISIT_INPATIENT,  D1, D2),
            (12, 1, VISIT_ER,         D1, D2),
            (13, 1, VISIT_OUTPATIENT, D1, D2),
        ], VISIT_SCHEMA)
        cond = spark.createDataFrame([
            (100, 1, CONDITION_DIABETES,     D1, None),
            (101, 1, CONDITION_HYPERTENSION, D1, None),
        ], CONDITION_SCHEMA)
        drug = spark.createDataFrame([
            (200, 1, 1503184, D1, D2, 30, 1.0),
        ], DRUG_SCHEMA)
        meas = spark.createDataFrame([
            (300, 1, MEASUREMENT_HBA1C, date(2019, 1, 1), 6.0),
            (301, 1, MEASUREMENT_HBA1C, date(2021, 1, 1), 7.5),
            (302, 1, MEASUREMENT_SBP,   D1, 120.0),
        ], MEASUREMENT_SCHEMA)
        rows = build_analytic_person(person, visit, cond, drug, meas).collect()
        return {row["person_id"]: row for row in rows}

    def test_one_row_per_person(self, result):
        assert len(result) == 2

    def test_age_computed_from_reference_date(self, result):
        assert result[1]["age"] == 45

    def test_year_of_birth_band(self, result):
        assert result[2]["year_of_birth_band"] == "1980s"

    def test_visit_counts_aggregated(self, result):
        assert result[1]["total_visit_count"] == 4
        assert result[1]["outpatient_visit_count"] == 2
        assert result[1]["inpatient_visit_count"] == 1
        assert result[1]["er_visit_count"] == 1

    def test_condition_flags(self, result):
        assert result[1]["has_diabetes"] is True
        assert result[1]["has_hypertension"] is True
        assert result[1]["condition_count"] == 2

    def test_no_conditions_flags_false(self, result):
        assert result[2]["has_diabetes"] is False
        assert result[2]["has_hypertension"] is False

    def test_person_with_no_visits_gets_zero_counts(self, result):
        assert result[2]["total_visit_count"] == 0
        assert result[2]["outpatient_visit_count"] == 0
        assert result[2]["inpatient_visit_count"] == 0
        assert result[2]["er_visit_count"] == 0

    def test_latest_hba1c_most_recent(self, result):
        assert result[1]["latest_hba1c"] == pytest.approx(7.5)

    def test_latest_systolic_bp(self, result):
        assert result[1]["latest_systolic_bp"] == pytest.approx(120.0)

    def test_no_measurements_gives_null_hba1c(self, result):
        assert result[2]["latest_hba1c"] is None
        assert result[2]["latest_systolic_bp"] is None

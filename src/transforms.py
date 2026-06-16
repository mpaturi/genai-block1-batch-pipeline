"""
Cleaning step and analytic_person build for the Block 1 OMOP pipeline.

Public API
----------
clean_all()             — drop dirty rows from all six raw tables; returns a
                          CleanedTables dataclass and CleaningMetrics with
                          before/after row counts per table.
build_analytic_person() — join and aggregate the cleaned tables into the
                          person-level analytic output DataFrame (plus a
                          year_of_birth_band column used as the Parquet partition key).

Cleaning order
--------------
PERSON is cleaned first (nulls + duplicates).  Downstream orphan checks use the
*cleaned* PERSON set, so an injected orphan person_id is never inherited by
child tables.  VISIT_OCCURRENCE is cleaned second for the same reason: NOTE's
orphan check on visit_occurrence_id uses the already-cleaned visit set.

No data is written here.  The caller (pipeline.py) is responsible for
re-running validate_all() on the cleaned tables as a hard gate before writing
to data/processed/.
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.concepts import (
    CONDITION_DIABETES,
    CONDITION_HYPERTENSION,
    MEASUREMENT_HBA1C,
    MEASUREMENT_SBP,
    VISIT_ER,
    VISIT_INPATIENT,
    VISIT_OUTPATIENT,
)
from src.config import REFERENCE_DATE


# ---------------------------------------------------------------------------
# Return-value containers
# ---------------------------------------------------------------------------

@dataclass
class CleanedTables:
    person: DataFrame
    visit: DataFrame
    condition: DataFrame
    drug: DataFrame
    measurement: DataFrame
    note: DataFrame


@dataclass
class CleaningMetrics:
    before: dict  # table_name -> raw row count
    after: dict   # table_name -> cleaned row count


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _dedup(df: DataFrame, pk: str) -> DataFrame:
    return df.dropDuplicates([pk])


def _drop_nulls(df: DataFrame, *cols: str) -> DataFrame:
    return df.dropna(subset=list(cols))


def _drop_bad_dates(df: DataFrame, start_col: str, end_col: str) -> DataFrame:
    """Remove rows where end_date < start_date (rows with null end_date are kept)."""
    return df.filter(F.col(end_col).isNull() | (F.col(end_col) >= F.col(start_col)))


def _drop_neg(df: DataFrame, col: str) -> DataFrame:
    return df.filter(F.col(col) >= 0)


def _drop_orphans(
    df: DataFrame, fk_col: str, parent: DataFrame, parent_pk: str
) -> DataFrame:
    """Remove rows whose non-null FK value is absent from the parent table."""
    valid = parent.select(F.col(parent_pk).alias("__pk")).distinct()
    return (
        df.join(valid, df[fk_col] == valid["__pk"], "left")
          .filter(F.col(fk_col).isNull() | F.col("__pk").isNotNull())
          .drop("__pk")
    )


# ---------------------------------------------------------------------------
# Per-table cleaning functions
# ---------------------------------------------------------------------------

def clean_person(df: DataFrame) -> DataFrame:
    df = _drop_nulls(df, "gender_concept_id", "year_of_birth", "race_concept_id", "ethnicity_concept_id")
    return _dedup(df, "person_id")


def clean_visit_occurrence(df: DataFrame, person: DataFrame) -> DataFrame:
    df = _drop_nulls(df, "person_id", "visit_concept_id", "visit_start_date", "visit_end_date")
    df = _drop_bad_dates(df, "visit_start_date", "visit_end_date")
    df = _drop_orphans(df, "person_id", person, "person_id")
    return _dedup(df, "visit_occurrence_id")


def clean_condition_occurrence(df: DataFrame) -> DataFrame:
    df = _drop_nulls(df, "person_id", "condition_concept_id", "condition_start_date")
    df = _drop_bad_dates(df, "condition_start_date", "condition_end_date")
    return _dedup(df, "condition_occurrence_id")


def clean_drug_exposure(df: DataFrame) -> DataFrame:
    df = _drop_nulls(df, "person_id", "drug_concept_id", "drug_exposure_start_date", "days_supply", "quantity")
    df = _drop_bad_dates(df, "drug_exposure_start_date", "drug_exposure_end_date")
    df = _drop_neg(df, "days_supply")
    df = _drop_neg(df, "quantity")
    return _dedup(df, "drug_exposure_id")


def clean_measurement(df: DataFrame, person: DataFrame) -> DataFrame:
    df = _drop_nulls(df, "person_id", "measurement_concept_id", "measurement_date", "value_as_number")
    df = _drop_neg(df, "value_as_number")
    df = _drop_orphans(df, "person_id", person, "person_id")
    return _dedup(df, "measurement_id")


def clean_note(df: DataFrame, visit: DataFrame) -> DataFrame:
    df = _drop_nulls(df, "person_id", "note_date", "note_text")
    df = _drop_orphans(df, "visit_occurrence_id", visit, "visit_occurrence_id")
    return _dedup(df, "note_id")


# ---------------------------------------------------------------------------
# Aggregate entry point for cleaning
# ---------------------------------------------------------------------------

def clean_all(
    person: DataFrame,
    visit: DataFrame,
    condition: DataFrame,
    drug: DataFrame,
    measurement: DataFrame,
    note: DataFrame,
) -> tuple[CleanedTables, CleaningMetrics]:
    """Clean all six tables and return cleaned DataFrames plus before/after counts."""
    before = {
        "person": person.count(),
        "visit_occurrence": visit.count(),
        "condition_occurrence": condition.count(),
        "drug_exposure": drug.count(),
        "measurement": measurement.count(),
        "note": note.count(),
    }

    clean_p = clean_person(person)
    clean_v = clean_visit_occurrence(visit, clean_p)
    clean_c = clean_condition_occurrence(condition)
    clean_d = clean_drug_exposure(drug)
    clean_m = clean_measurement(measurement, clean_p)
    clean_n = clean_note(note, clean_v)

    after = {
        "person": clean_p.count(),
        "visit_occurrence": clean_v.count(),
        "condition_occurrence": clean_c.count(),
        "drug_exposure": clean_d.count(),
        "measurement": clean_m.count(),
        "note": clean_n.count(),
    }

    tables = CleanedTables(
        person=clean_p,
        visit=clean_v,
        condition=clean_c,
        drug=clean_d,
        measurement=clean_m,
        note=clean_n,
    )
    return tables, CleaningMetrics(before=before, after=after)


# ---------------------------------------------------------------------------
# analytic_person build
# ---------------------------------------------------------------------------

def build_analytic_person(
    person: DataFrame,
    visit: DataFrame,
    condition: DataFrame,
    drug: DataFrame,
    measurement: DataFrame,
) -> DataFrame:
    """
    Aggregate cleaned tables into one row per person.

    Returns a DataFrame with all analytic_person columns plus year_of_birth_band
    (the Parquet partition key).  The caller should write this with
    .write.partitionBy("year_of_birth_band").
    """
    ref_year = REFERENCE_DATE.year

    # --- visit counts -------------------------------------------------------
    visit_agg = visit.groupBy("person_id").agg(
        F.count("*").alias("total_visit_count"),
        F.sum(F.when(F.col("visit_concept_id") == VISIT_OUTPATIENT, 1).otherwise(0))
         .alias("outpatient_visit_count"),
        F.sum(F.when(F.col("visit_concept_id") == VISIT_INPATIENT, 1).otherwise(0))
         .alias("inpatient_visit_count"),
        F.sum(F.when(F.col("visit_concept_id") == VISIT_ER, 1).otherwise(0))
         .alias("er_visit_count"),
    )

    # --- condition flags -----------------------------------------------------
    condition_agg = condition.groupBy("person_id").agg(
        F.count("*").alias("condition_count"),
        F.max(F.col("condition_concept_id") == CONDITION_DIABETES).alias("has_diabetes"),
        F.max(F.col("condition_concept_id") == CONDITION_HYPERTENSION).alias("has_hypertension"),
    )

    # --- drug count ----------------------------------------------------------
    drug_agg = drug.groupBy("person_id").agg(
        F.count("*").alias("drug_exposure_count"),
    )

    # --- measurement counts + latest date ------------------------------------
    meas_agg = measurement.groupBy("person_id").agg(
        F.count("*").alias("measurement_count"),
        F.max("measurement_date").alias("latest_measurement_date"),
    )

    # --- latest HbA1c and systolic BP (most recent value per person) ---------
    w = Window.partitionBy("person_id").orderBy(F.col("measurement_date").desc())

    hba1c = (
        measurement
        .filter(F.col("measurement_concept_id") == MEASUREMENT_HBA1C)
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select("person_id", F.col("value_as_number").alias("latest_hba1c"))
    )

    sbp = (
        measurement
        .filter(F.col("measurement_concept_id") == MEASUREMENT_SBP)
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select("person_id", F.col("value_as_number").alias("latest_systolic_bp"))
    )

    # decade band: 1943 -> "1940s", 1987 -> "1980s"
    year_band = F.concat(
        ((F.col("year_of_birth") / 10).cast("int") * 10).cast("string"),
        F.lit("s"),
    )

    # --- assemble -----------------------------------------------------------
    return (
        person
        .withColumn("age", F.lit(ref_year) - F.col("year_of_birth"))
        .withColumn("year_of_birth_band", year_band)
        .join(visit_agg, "person_id", "left")
        .join(condition_agg, "person_id", "left")
        .join(drug_agg, "person_id", "left")
        .join(meas_agg, "person_id", "left")
        .join(hba1c, "person_id", "left")
        .join(sbp, "person_id", "left")
        .select(
            "person_id",
            "age",
            "year_of_birth_band",
            "gender_concept_id",
            F.coalesce(F.col("total_visit_count"), F.lit(0)).cast("long").alias("total_visit_count"),
            F.coalesce(F.col("outpatient_visit_count"), F.lit(0)).cast("long").alias("outpatient_visit_count"),
            F.coalesce(F.col("inpatient_visit_count"), F.lit(0)).cast("long").alias("inpatient_visit_count"),
            F.coalesce(F.col("er_visit_count"), F.lit(0)).cast("long").alias("er_visit_count"),
            F.coalesce(F.col("condition_count"), F.lit(0)).cast("long").alias("condition_count"),
            F.coalesce(F.col("drug_exposure_count"), F.lit(0)).cast("long").alias("drug_exposure_count"),
            F.coalesce(F.col("measurement_count"), F.lit(0)).cast("long").alias("measurement_count"),
            F.coalesce(F.col("has_diabetes"), F.lit(False)).alias("has_diabetes"),
            F.coalesce(F.col("has_hypertension"), F.lit(False)).alias("has_hypertension"),
            "latest_hba1c",
            "latest_systolic_bp",
            "latest_measurement_date",
        )
    )

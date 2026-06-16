"""PySpark StructType schemas for all six raw OMOP-style tables and analytic_person.

Used by io_utils.py (read with explicit schema) and transforms.py (output validation).
Nullability mirrors docs/spec.md: required fields are nullable=False, optional FK/lookup
columns are nullable=True.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

PERSON = StructType([
    StructField("person_id", IntegerType(), nullable=False),
    StructField("gender_concept_id", IntegerType(), nullable=False),
    StructField("year_of_birth", IntegerType(), nullable=False),
    StructField("race_concept_id", IntegerType(), nullable=False),
    StructField("ethnicity_concept_id", IntegerType(), nullable=False),
    StructField("location_id", IntegerType(), nullable=True),
])

VISIT_OCCURRENCE = StructType([
    StructField("visit_occurrence_id", IntegerType(), nullable=False),
    StructField("person_id", IntegerType(), nullable=False),
    StructField("visit_concept_id", IntegerType(), nullable=False),
    StructField("visit_start_date", DateType(), nullable=False),
    StructField("visit_end_date", DateType(), nullable=False),
    StructField("care_site_id", IntegerType(), nullable=True),
    StructField("provider_id", IntegerType(), nullable=True),
])

CONDITION_OCCURRENCE = StructType([
    StructField("condition_occurrence_id", IntegerType(), nullable=False),
    StructField("person_id", IntegerType(), nullable=False),
    StructField("condition_concept_id", IntegerType(), nullable=False),
    StructField("condition_start_date", DateType(), nullable=False),
    StructField("condition_end_date", DateType(), nullable=True),
    StructField("visit_occurrence_id", IntegerType(), nullable=True),
])

DRUG_EXPOSURE = StructType([
    StructField("drug_exposure_id", IntegerType(), nullable=False),
    StructField("person_id", IntegerType(), nullable=False),
    StructField("drug_concept_id", IntegerType(), nullable=False),
    StructField("drug_exposure_start_date", DateType(), nullable=False),
    StructField("drug_exposure_end_date", DateType(), nullable=True),
    StructField("days_supply", IntegerType(), nullable=False),
    StructField("quantity", DoubleType(), nullable=False),
])

MEASUREMENT = StructType([
    StructField("measurement_id", IntegerType(), nullable=False),
    StructField("person_id", IntegerType(), nullable=False),
    StructField("measurement_concept_id", IntegerType(), nullable=False),
    StructField("measurement_date", DateType(), nullable=False),
    StructField("value_as_number", DoubleType(), nullable=False),
    StructField("unit_concept_id", IntegerType(), nullable=True),
    StructField("visit_occurrence_id", IntegerType(), nullable=True),
])

NOTE = StructType([
    StructField("note_id", IntegerType(), nullable=False),
    StructField("person_id", IntegerType(), nullable=False),
    StructField("note_date", DateType(), nullable=False),
    StructField("note_text", StringType(), nullable=False),
    StructField("visit_occurrence_id", IntegerType(), nullable=True),
])

# Output of transforms.py; partitioned Parquet under data/processed/analytic_person/.
# Count columns are LongType to match Spark's count() return type.
ANALYTIC_PERSON = StructType([
    StructField("person_id", IntegerType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("gender_concept_id", IntegerType(), nullable=False),
    StructField("total_visit_count", LongType(), nullable=False),
    StructField("outpatient_visit_count", LongType(), nullable=False),
    StructField("inpatient_visit_count", LongType(), nullable=False),
    StructField("er_visit_count", LongType(), nullable=False),
    StructField("condition_count", LongType(), nullable=False),
    StructField("drug_exposure_count", LongType(), nullable=False),
    StructField("measurement_count", LongType(), nullable=False),
    StructField("has_diabetes", BooleanType(), nullable=False),
    StructField("has_hypertension", BooleanType(), nullable=False),
    StructField("latest_hba1c", DoubleType(), nullable=True),
    StructField("latest_systolic_bp", DoubleType(), nullable=True),
    StructField("latest_measurement_date", DateType(), nullable=True),
])

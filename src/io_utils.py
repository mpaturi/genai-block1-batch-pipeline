"""Spark I/O helpers for Block 1.

read_*() functions load each raw CSV from data/raw/ using the explicit schemas
from schemas.py.  write_parquet() writes a DataFrame to partitioned Parquet
under data/processed/.
"""

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src import schemas
from src.config import PROCESSED_DIR, RAW_DIR


def get_spark_session(app_name: str = "block1-pipeline") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _read_csv(spark: SparkSession, filename: str, schema) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(str(RAW_DIR / filename))
    )


def read_person(spark: SparkSession) -> DataFrame:
    return _read_csv(spark, "person.csv", schemas.PERSON)


def read_visit_occurrence(spark: SparkSession) -> DataFrame:
    return _read_csv(spark, "visit_occurrence.csv", schemas.VISIT_OCCURRENCE)


def read_condition_occurrence(spark: SparkSession) -> DataFrame:
    return _read_csv(spark, "condition_occurrence.csv", schemas.CONDITION_OCCURRENCE)


def read_drug_exposure(spark: SparkSession) -> DataFrame:
    return _read_csv(spark, "drug_exposure.csv", schemas.DRUG_EXPOSURE)


def read_measurement(spark: SparkSession) -> DataFrame:
    return _read_csv(spark, "measurement.csv", schemas.MEASUREMENT)


def read_note(spark: SparkSession) -> DataFrame:
    return _read_csv(spark, "note.csv", schemas.NOTE)


def write_parquet(
    df: DataFrame,
    path: Path = PROCESSED_DIR,
    partition_by: list[str] | None = None,
    mode: str = "overwrite",
) -> None:
    writer = df.write.mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(str(path))

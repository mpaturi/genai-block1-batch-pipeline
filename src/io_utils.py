"""Spark I/O helpers for Block 1.

read_*() functions load each raw CSV from data/raw/ using the explicit schemas
from schemas.py.  write_parquet() writes a DataFrame to partitioned Parquet
under data/processed/.
"""

import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src import schemas
from src.config import PROCESSED_DIR, RAW_DIR

_JAVA21_HOME = r"C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot"
_HADOOP_HOME = os.path.join(os.path.expanduser("~"), "hadoop")


def get_spark_session(app_name: str = "block1-pipeline") -> SparkSession:
    # PySpark 4.1 / Hadoop 3.4 calls Subject.getSubject(), which Java 25
    # removed entirely.  Pin to Java 21 LTS where it still works.
    if os.path.isdir(_JAVA21_HOME):
        os.environ["JAVA_HOME"] = _JAVA21_HOME
    if os.path.isdir(_HADOOP_HOME):
        os.environ["HADOOP_HOME"] = _HADOOP_HOME
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
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
    import platform
    if platform.system() == "Windows":
        _write_parquet_pandas(df, path, partition_by, mode)
    else:
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(str(path))


def _write_parquet_pandas(
    df: DataFrame,
    path: Path,
    partition_by: list[str] | None,
    mode: str,
) -> None:
    """Pandas/PyArrow fallback — avoids Hadoop NativeIO on Windows."""
    import shutil

    import pyarrow as pa
    import pyarrow.parquet as pq

    if mode == "overwrite" and path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(df.toPandas())
    if partition_by:
        pq.write_to_dataset(table, root_path=str(path), partition_cols=partition_by)
    else:
        pq.write_table(table, str(path / "part-00000.parquet"))

import os

import pytest
from pyspark.sql import SparkSession

from src.io_utils import _HADOOP_HOME, _JAVA21_HOME


@pytest.fixture(scope="session")
def spark():
    if os.path.isdir(_JAVA21_HOME):
        os.environ["JAVA_HOME"] = _JAVA21_HOME
    if os.path.isdir(_HADOOP_HOME):
        os.environ["HADOOP_HOME"] = _HADOOP_HOME
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("block1-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()

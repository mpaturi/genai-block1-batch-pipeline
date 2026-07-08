import pytest

from src.io_utils import get_spark_session


@pytest.fixture(scope="session")
def spark():
    session = get_spark_session(app_name="block1-tests", master="local[1]", ui_enabled=False)
    session.conf.set("spark.sql.codegen.wholeStage", "false")
    yield session
    session.stop()
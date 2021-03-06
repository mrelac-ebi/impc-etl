"""
Conftest: configuration for testing
"""
import logging
import pytest
import findspark
findspark.init()
# pylint:disable=C0413
from pyspark import SparkConf
from pyspark.sql import SparkSession
from impc_etl.config import SparkConfig


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_session(request):
    """
    fixture for creating a spark session

    :param request: pytest.FixtureRequest object
    :return spark: pyspark.sql.SparkSession
    """
    conf = SparkConf().setAll([('spark.jars.packages', ','.join(SparkConfig.SPARK_JAR_PACKAGES))])
    spark = SparkSession.builder.appName("IMPC_ETL_TEST").config(conf=conf).getOrCreate()
    request.addfinalizer(spark.stop)
    quiet_py4j()
    return spark

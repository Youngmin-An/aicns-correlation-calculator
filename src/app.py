"""
    Script-oriented co-feature correlation calcultor's client module
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pendulum
import os
import logging

logger = logging.getLogger(__name__)


def get_conf_from_evn():
    """
        Get conn info from env variables
    :return:
    """
    conf = dict()
    try:
        # HDFS
        conf["HDFS_HOST"] = os.getenv("HDFS_HOST")
        conf["HDFS_PORT"] = os.getenv("HDFS_PORT")
        start_datetime = os.getenv("APP_TIME_START")  # yyyy-MM-dd'T'HH:mm:ss
        end_datetime = os.getenv("APP_TIME_END")  #  yyyy-MM-dd'T'HH:mm:ss
        # Raw data period
        conf["start"] = pendulum.parse(start_datetime)
        conf["end"] = pendulum.parse(end_datetime)
    except Exception as e:
        raise e
    logger.info(f"Got conf: {conf}")
    return conf


spark_conf = SparkConf().setAppName("aicns-correlation-calculator-")  # todo identify app name
spark = SparkContext(conf=spark_conf)
sql_context = SQLContext(spark)

app_conf = get_conf_from_evn()

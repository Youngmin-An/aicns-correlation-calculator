"""
    Script-oriented co-feature correlation calcultor's client module
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import os
import logging

# logger = logging.getLogger(__name__)


def get_conf_from_evn():
    conf = dict()
    try:
        conf["HDFS_HOST"] = os.getenv("HDFS_HOST")
        conf["HDFS_PORT"] = os.getenv("HDFS_PORT")
    except Exception as e:
        raise e
    return conf


spark_conf = SparkConf().setAppName("aicns-correlation-calculator-")  # todo identify app name
spark = SparkContext(conf=spark_conf)
sql_context = SQLContext(spark)

app_conf = get_conf_from_evn()

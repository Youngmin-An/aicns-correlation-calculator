"""
    Script-oriented co-feature correlation calcultor's client module
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pendulum
import os
import logging
import uuid
from featureMetadataFetcher import FeatureMetadataFetcher, SensorMetadataFetcher
from rawDataLoader import RawDataLoader, DatePartitionedRawDataLoader

logger = logging.getLogger(__name__)


def get_conf_from_evn():
    """
        Get conn info from env variables
    :return:
    """
    conf = dict()
    try:
        # Metadata
        conf["METADATA_HOST"] = os.getenv("METADATA_HOST")
        conf["METADATA_PORT"] = os.getenv("METADATA_PORT")
        conf["METADATA_TYPE"] = os.getenv("METADATA_TYPE", default="sensor")
        conf["METADATA_BACKEND"] = os.getenv("METADATA_BACKEND", default="MongoDB")
        # Data source
        conf["SOURCE_HOST"] = os.getenv("SOURCE_HOST")
        conf["SOURCE_PORT"] = os.getenv("SOURCE_PORT")
        conf["SOURCE_BACKEND"] = os.getenv("SOURCE_BACKEND", default="HDFS")
        # Raw data period
        start_datetime = os.getenv("APP_TIME_START")  # yyyy-MM-dd'T'HH:mm:ss
        end_datetime = os.getenv("APP_TIME_END")  #  yyyy-MM-dd'T'HH:mm:ss
        conf["start"] = pendulum.parse(start_datetime)
        conf["end"] = pendulum.parse(end_datetime)
    except Exception as e:
        raise e
    logger.info(f"Got conf: {conf}")
    return conf


app_conf = get_conf_from_evn()

# Spark session
spark_conf = SparkConf().setAppName(
    f"aicns-correlation-calculator-{uuid.uuid4}"
)  # todo experiment parameter-based app name
spark = SparkContext(conf=spark_conf)
sql_context = SQLContext(spark)
app_conf["sql_context"] = sql_context

# Fetch metadata
fetcher: FeatureMetadataFetcher = SensorMetadataFetcher()
fetcher.get_or_create_conn(app_conf)
sensors, positions = fetcher.fetch_metadata()

# Load raw data
loader: RawDataLoader = DatePartitionedRawDataLoader()
loader.prepare_to_load(**app_conf)
data = dict()
for position in positions:
    data[str(position.pos_id)] = loader.load_data_in_a_cluster(start=app_conf["start"], end=app_conf["end"], cluster=position)


# Tier-down Spark
spark.stop()

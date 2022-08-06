import logging

from pipelines.unemployment_etl.utils.unemployment_utils import get_spark_session


def create_mirror_table(raw_data_path: str):
    spark = get_spark_session()
    df = spark.read.json(raw_data_path)
    logging.info("Read raw data")

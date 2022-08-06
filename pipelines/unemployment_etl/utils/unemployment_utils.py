import logging
import os

from pyspark.sql import SparkSession


def set_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )


def prepare_datalake_dirs():
    try:
        os.mkdir('./datalake')
    except FileExistsError as f:
        pass
    try:
        os.mkdir('./datalake/raw_data')
    except FileExistsError as f:
        pass
    try:
        os.mkdir('./datalake/mirror_table')
    except FileExistsError as f:
        pass
    try:
        os.mkdir('./datalake/processed_data')
    except FileExistsError as f:
        pass


def get_spark_session():
    spark = SparkSession \
        .builder \
        .appName("create_mirror_table") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

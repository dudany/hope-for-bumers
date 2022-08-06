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


def prepare_datalake(spark:SparkSession):
    try:
        os.mkdir('./datalake')
    except FileExistsError as f:
        pass
    try:
        os.mkdir('./datalake/raw_data')
    except FileExistsError as f:
        pass
    try:
        os.mkdir('./datalake/processed_data')
    except FileExistsError as f:
        pass
    # running mirror table ddl
    with open("./pipelines/unemployment_etl/create_mirror_table/mirror_table_ddl.sql") as f:
        ddl = f.read()
    spark.sql(ddl)

def get_spark_session():
    spark = SparkSession \
        .builder \
        .appName("create_mirror_table") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

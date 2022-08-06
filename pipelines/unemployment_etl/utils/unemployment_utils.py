import configparser
import logging
import os
from typing import Dict

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def set_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler()
        ]
    )


def create_dir(rel_path):
    abs_path = os.path.join(os.getcwd(), rel_path)
    try:
        os.mkdir(abs_path)
    except FileExistsError as f:
        pass
    finally:
        return abs_path


def prepare_datalake(spark: SparkSession) -> Dict:
    """
    Creates data lake dirs and mirror delta lake table
    :param spark: spark to run delta lake ddl
    :return: dict of the absolute paths of the delta lake
    """
    datalake_dirs = {'datalake': 'datalake',
                     "raw_data": "datalake/raw_data",
                     "processed_data": "datalake/processed_data"}
    paths_dict = {dir: create_dir(datalake_dirs[dir]) for dir in datalake_dirs}
    # running mirror table ddl
    with open("./pipelines/unemployment_etl/create_mirror_table/mirror_table_ddl.sql") as f:
        ddl = f.read()
    spark.sql(ddl)

    return paths_dict


def get_spark_session():
    spark = SparkSession \
        .builder \
        .appName("create_mirror_table") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark


def create_pipeline_spark_context() -> SparkSession:
    builder = SparkSession \
        .builder \
        .appName("hope-for-bumers-etl") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .config("spark.sql.warehouse.dir", "./datalake")

    return configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()


def get_defaults(path_to_config_file) -> tuple:
    config = configparser.ConfigParser()
    config.read(path_to_config_file)
    default_dates = (config.get("DEFAULT","startyear"), config.get("DEFAULT","endyear"))
    default_series_id = config.get("DEFAULT","seriesId")
    return default_dates, default_series_id

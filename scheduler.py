from delta import *
from pyspark.sql import SparkSession
from pipelines.unemployment_etl.create_mirror_table.mirror_tables import create_mirror_table
from pipelines.unemployment_etl.raw_data_ingestion.ingestion_dumper import ingest_raw_data
from pipelines.unemployment_etl.utils.unemployment_utils import set_logging, prepare_datalake

set_logging()


def create_pipeline_spark_context() -> SparkSession:
    builder = SparkSession \
        .builder \
        .appName("hope-for-bumers-etl") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .config("spark.sql.warehouse.dir", "./datalake")

    return configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()


if __name__ == '__main__':
    # todo move to config
    DEFAULT_SERIES_ID = ['LAUCT395688200000003', 'LAUCT394713800000003', 'LAUCT485235600000003', 'LAUCN271410000000003',
                         'LAUCN420410000000003']
    DEFAULT_DATES = {"startyear": "2019", "endyear": "2021"}
    raw_data_path = "/Users/ddani/Desktop/Dani/hope-for-bumers/datalake/raw_data"

    spark = create_pipeline_spark_context()
    prepare_datalake(spark)
    ingest_raw_data(spark=spark, raw_data_location=raw_data_path, date_period=DEFAULT_DATES,
                    series_list=DEFAULT_SERIES_ID)
    create_mirror_table(spark, raw_data_path)

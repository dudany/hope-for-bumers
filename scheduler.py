import os
from typing import Tuple
import click as click
import logging
from pipelines.unemployment_etl.create_mirror_table.mirror_tables import create_mirror_table
from pipelines.unemployment_etl.raw_data_ingestion.ingestion_dumper import ingest_raw_data
from pipelines.unemployment_etl.utils.unemployment_utils import set_logging, prepare_datalake, \
    create_pipeline_spark_context, get_defaults

set_logging()
PATH_TO_CONFIG_FILE = f"{os.getcwd()}/pipelines/unemployment_etl/configs.ini"

# get default values for etl set in ini file
DEFAULT_DATES, DEFAULT_SERIES_ID = get_defaults(PATH_TO_CONFIG_FILE)


@click.command(name="schedule_pipeline")
@click.option('--date_period', nargs=2, type=str, default=DEFAULT_DATES, help='Enter start and end years')
@click.option('--series_id', type=str, default=DEFAULT_SERIES_ID, help='enter series id comma separated')
@click.option("--skip_raw_data", is_flag=True, show_default=True, default=False, help="skip raw_data step")
@click.option("--skip_mirror_table", is_flag=True, show_default=True, default=False, help="skip mirror_table step")
def schedule_unemployment_pipeline(date_period: Tuple, series_id: str, skip_raw_data: bool, skip_mirror_table: bool):
    etl_date = {"startyear": date_period[0], "endyear": date_period[1]}

    if skip_mirror_table and skip_raw_data:
        logging.warning("Skiping raw data ingestion and mirror creating, running infra preparation only")
    spark = create_pipeline_spark_context()
    paths_dict = prepare_datalake(spark)

    if not skip_raw_data:
        series_id_list = series_id.split(",")
        logging.info(f"Ingesting dates: {date_period[0]} - {date_period[1]}")
        ingest_raw_data(raw_data_location=paths_dict["raw_data"], date_period=etl_date, series_list=series_id_list)
    if not skip_mirror_table:
        create_mirror_table(spark, paths_dict["raw_data"], paths_dict["processed_data"])


if __name__ == '__main__':
    schedule_unemployment_pipeline()

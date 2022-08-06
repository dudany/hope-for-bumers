from pipelines.unemployment_etl.create_mirror_table.mirror_tables import create_mirror_table
from pipelines.unemployment_etl.raw_data_ingestion.ingestion_dumper import ingest_raw_data
from pipelines.unemployment_etl.utils.unemployment_utils import set_logging, prepare_datalake, \
    create_pipeline_spark_context

set_logging()

if __name__ == '__main__':
    # todo move to config
    DEFAULT_SERIES_ID = ['LAUCT395688200000003', 'LAUCT394713800000003', 'LAUCT485235600000003', 'LAUCN271410000000003',
                         'LAUCN420410000000003']
    DEFAULT_DATES = {"startyear": "2019", "endyear": "2021"}

    spark = create_pipeline_spark_context()
    paths_dict = prepare_datalake(spark)
    ingest_raw_data(raw_data_location=paths_dict["raw_data"], date_period=DEFAULT_DATES, series_list=DEFAULT_SERIES_ID)
    create_mirror_table(spark, paths_dict["raw_data"], paths_dict["processed_data"])

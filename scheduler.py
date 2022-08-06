from pipelines.unemployment_etl.create_mirror_table.mirror_tables import create_mirror_table
from pipelines.unemployment_etl.raw_data_ingestion.ingestion_dumper import ingest_raw_data
from pipelines.unemployment_etl.utils.unemployment_utils import set_logging, prepare_datalake_dirs

set_logging()

if __name__ == '__main__':
    # todo move to config
    # DEFAULT_SERIES_ID = ['LAUCT395688200000003', 'LAUCT394713800000003', 'LAUCT485235600000003', 'LAUCN271410000000003',
    #                      'LAUCN420410000000003']
    DEFAULT_SERIES_ID = ['LAUCT395688200000003', 'LAUCT394713800000003']
    DEFAULT_DATES = {"startyear": "2019", "endyear": "2021"}
    raw_data_path = "/Users/ddani/Desktop/Dani/hope-for-bumers/datalake/raw_data"

    prepare_datalake_dirs()
    # ingest_raw_data(raw_data_path, DEFAULT_DATES, DEFAULT_SERIES_ID)
    create_mirror_table(raw_data_path)

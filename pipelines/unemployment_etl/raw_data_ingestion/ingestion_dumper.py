import datetime

import pandas as pd
import requests
import json
import logging
from pyspark.sql import SparkSession
from retrying import retry
from pipelines.unemployment_etl.utils.unemployment_utils import set_logging
from typing import Dict, List

set_logging()

headers = {'Content-type': 'application/json'}
BUREAU_OF_LABOR_STATISTICS_API_HEADER = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'


@retry(stop_max_attempt_number=3, wait_fixed=2000)  # retry api call
def get_data_from_api(data: str) -> str:
    logging.info("Making API call to get labor statistics data")
    return requests.post(BUREAU_OF_LABOR_STATISTICS_API_HEADER, data=data, headers=headers).text


def ingest_raw_data(spark: SparkSession, raw_data_location: str, date_period: Dict[str, str], series_list: List[str]):
    """

    :param spark: spark session created for pipeline
    :param raw_data_location: String of path for the raw data to land in
    :param date_period: Dictionary with keys `startyear` and `endyear` that representing the required date period
    :param series_list: List of required series ids to process
    """
    logging.info("Starting ingestion of raw data")
    # handles input values
    header_dict = {"seriesid": series_list}
    header_dict.update(date_period)
    data = json.dumps(header_dict)
    total_series_df = pd.DataFrame()
    # making api call to ge data
    json_data = json.loads(get_data_from_api(data))
    download_date = datetime.datetime.now().date().isoformat()
    # processing data from response
    for series in json_data['Results']['series']:
        rows_list = []
        seriesId = series['seriesID']
        for item in series['data']:
            footnotes = ""
            for footnote in item['footnotes']:
                if footnote:
                    footnotes = footnotes + footnote['text'] + ','
            if 'M01' <= item['period'] <= 'M12':
                rows_list.append(
                    {"series_id": seriesId, "year": item['year'], "period": item['period'], "value": item['value'],
                     "footnotes": footnotes[0:-1]})
        series_df = pd.DataFrame(rows_list)
        # writing to raw data location
        total_series_df = pd.concat([total_series_df, series_df])

    spark_df = spark.createDataFrame(total_series_df).withColumn("received_date", download_date)

    # repartition to reduce number of files and each json file per series_id
    spark_df.repartition(len(series_list), 'series_id').write.json(raw_data_location, mode="append")
    logging.info(f"Written files to: {raw_data_location}")
    logging.info("Finished data ingestion")

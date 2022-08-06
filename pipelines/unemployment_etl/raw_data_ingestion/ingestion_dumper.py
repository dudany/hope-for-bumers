import datetime
import os
import pandas as pd
import requests
import json
import logging
from retrying import retry
from pipelines.unemployment_etl.utils.unemployment_utils import set_logging
from typing import Dict, List

set_logging()

headers = {'Content-type': 'application/json'}
BUREAU_OF_LABOR_STATISTICS_API_HEADER = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'


class RequestNotProcessed(Exception):
    pass


@retry(stop_max_attempt_number=3, wait_fixed=2000)  # retry api call
def get_data_from_api(data: str) -> str:
    logging.info("Making API call to get labor statistics data")
    return requests.post(BUREAU_OF_LABOR_STATISTICS_API_HEADER, data=data, headers=headers).text


def ingest_raw_data(raw_data_location: str, date_period: Dict[str, str], series_list: List[str]) -> str:
    """
    :param spark: spark session created for pipeline
    :param raw_data_location: String of path for the raw data to land in
    :param date_period: Dictionary with keys `startyear` and `endyear` that representing the required date period
    :param series_list: List of required series ids to process
    :rtype: STRING returns path of raw file
    """
    logging.info("Starting ingestion of raw data")
    # handles input values
    header_dict = {"seriesid": series_list}
    header_dict.update(date_period)
    data = json.dumps(header_dict)
    total_series_df = pd.DataFrame()
    # making api call to ge data
    json_data = json.loads(get_data_from_api(data))
    download_ts = datetime.datetime.now()
    # processing data from response
    if json_data['status'] == 'REQUEST_NOT_PROCESSED':
        raise RequestNotProcessed(json_data['message'][0])

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

    total_series_df["received_date"] = download_ts.date().isoformat()
    # write the df to json lines file
    file_pattern = f"raw_data_{download_ts.strftime('%y_%m_%d_%H_%M_%S')}.json"
    file_path = os.path.join(raw_data_location, file_pattern)
    total_series_df.to_json(file_path, orient='records', lines=True)

    logging.info(f"Written files to: {raw_data_location}")
    logging.info("Finished data ingestion")
    return file_path

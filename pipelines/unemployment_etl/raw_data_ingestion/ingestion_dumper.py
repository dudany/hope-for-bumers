import pandas as pd
import requests
import json
import os
import logging
from retrying import retry
from pipelines.unemployment_etl.utils.unemployment_utils import set_logging
from typing import Dict, List

set_logging()

headers = {'Content-type': 'application/json'}
DEFAULT_SERIES_ID = ['LAUCT395688200000003', 'LAUCT394713800000003', 'LAUCT485235600000003', 'LAUCN271410000000003',
                     'LAUCN420410000000003']
DEFAULT_DATES = {"startyear": "2019", "endyear": "2021"}
BUREAU_OF_LABOR_STATISTICS_API_HEADER = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'


@retry(stop_max_attempt_number=3, wait_fixed=2000)
def get_data_from_api(data: str) -> str:
    logging.info("Making API call to get labor statistics data")
    return requests.post(BUREAU_OF_LABOR_STATISTICS_API_HEADER, data=data, headers=headers).text


def ingest_raw_data(raw_data_location: str, date_period: Dict[str, str] = None, series_list: List[str] = None):
    """

    :param raw_data_location: String of path for the raw data to land in
    :param date_period: Dictionary with keys `startyear` and `endyear` that representing the required date period
    :param series_list: List of required series ids to process
    """
    logging.info("Starting ingestion of raw data")
    # handles default values
    if series_list is None:
        series_list = DEFAULT_SERIES_ID
    if date_period is None:
        date_period = DEFAULT_DATES
    header_dict = {"seriesid": series_list}
    header_dict.update(date_period)
    data = json.dumps(header_dict)

    # making api call to ge data
    json_data = json.loads(get_data_from_api(data))
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
        file_pattern = f"{seriesId}_{date_period['startyear']}_{date_period['endyear']}.json"
        file_path = os.path.join(raw_data_location, file_pattern)
        # writing to raw data location
        series_df.to_json(file_path)
        logging.info(f"Written file: {file_pattern}")


if __name__ == '__main__':
    ingest_raw_data("/Users/ddani/Desktop/Dani/hope-for-bumers/datalake/raw_data")

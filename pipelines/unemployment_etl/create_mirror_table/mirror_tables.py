import logging
import os
import pyspark.sql.functions as F
from typing import List
from delta import DeltaTable
from pyspark.sql import SparkSession

MIRROR_TABLE_NAME = "mirror_table"
UPSERT_DICT = {
    "series_id": "raw.series_id",
    "year": "raw.year",
    "period": "raw.period",
    "value": "raw.value",
    "footnotes": "raw.footnotes",
    "received_date": "raw.received_date",
    "raw_file": "raw.raw_file"
}


def move_files_to_processed(processed_files: List, raw_data_path: str, processed_data_path: str):
    for f in processed_files:
        os.replace(os.path.join(raw_data_path, f), os.path.join(processed_data_path, f))


def write_mirror_table(spark: SparkSession, raw_data_path: str, processed_data_path: str):
    # check if the raw data folder is not empty
    if not len(os.listdir(raw_data_path)):
        logging.warning("Raw data Directory is empty")
    else:
        # reads current table data upsert ingested data
        logging.info("Starting upserting mirror to table")
        mirror_table_df = DeltaTable.forName(spark, tableOrViewName=MIRROR_TABLE_NAME)
        raw_df = spark.read.json(raw_data_path) \
            .withColumn("raw_file", F.element_at(F.split(F.input_file_name(), "/"), -1)) \
            .select("series_id", "year", "period", "value", "footnotes", "received_date", "raw_file")

        mirror_table_df.alias('mirror').merge(
            raw_df.alias('raw'),
            'mirror.series_id = raw.series_id AND mirror.year = raw.period AND mirror.year = raw.period') \
            .whenMatchedUpdate(set=UPSERT_DICT) \
            .whenNotMatchedInsertAll() \
            .execute()
        logging.info("Finished upserting mirror to table")
        processed_files = [row.raw_file for row in raw_df.select("raw_file").distinct().collect()]


        logging.info("Moving Raw files to processed")
        move_files_to_processed(processed_files, raw_data_path, processed_data_path)
        logging.info("Finished moving Raw files to processed")
    logging.info("Finished mirroring table")

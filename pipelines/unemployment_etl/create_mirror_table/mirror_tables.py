import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql import SparkSession

MIRROR_TABLE_CONST = "mirror_table"


def create_mirror_table(spark: SparkSession, raw_data_path: str):
    # reads current table data upsert ingested data
    mirror_table_df = DeltaTable.forName(spark, tableOrViewName=MIRROR_TABLE_CONST)
    raw_df = spark.read.json(raw_data_path).withColumn("raw_file",F.input_file_name())\
                .select("series_id", "year", "period", "value", "footnotes", "received_date")

    mirror_table_df.alias('mirror').merge(
        raw_df.alias('raw'),
        'mirror.series_id = raw.series_id AND mirror.year = raw.period AND mirror.year = raw.period') \
        .whenMatchedUpdate(set=
    {
        "series_id": "raw.series_id",
        "year": "raw.year",
        "period": "raw.period",
        "value": "raw.value",
        "footnotes": "raw.footnotes",
        "received_date": "raw.received_date",
        "raw_file": "raw.raw_file"
    }
    ) \
        .whenNotMatchedInsertAll() \
        .execute()

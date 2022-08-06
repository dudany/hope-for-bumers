CREATE TABLE IF NOT EXISTS `mirror_table`
(
    `series_id`     STRING,
    `year`          STRING,
    `period`        STRING,
    `value`         STRING,
    `footnotes`     STRING,
    `received_date` STRING,
    `raw_file`      STRING
) USING DELTA
    PARTITIONED BY ( `series_id`,`year`);

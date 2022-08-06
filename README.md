# hope-for-bumers

## pipeline flow & design
The pipeline is made out if 2 steps:
1. Ingesting the data using BLS api and saves it as .json files to raw data.
2. Create a mirror table that represent the unemployment ratio of each county for aspecific month.

The mirror table is saved as delta lake format as we need to support upserts in the data.
The unique keys of the table are: (series_id, year, period). 
Each run of the pipeline adds more daw data to the raw data dir. At the second step
the raw data is read and inserts new rows to table or updates rows with existing unique key (series_id, year, period).

The pipeline is activated using click cli, first a step to set the db environment is executed,
it creates the raw data file paths, the spark db where the mirror table is saved at and executes 
DDL to create delta lake table (mirror_table).
Later the raw data step is executed and mirror table creation after that(if specific flag are inserted steps can be skipped).

## preparation 
create virtual env that you will be using for this project and then run.
```shell
pip install -r requirements.txt
```

## pipeline activation
Be at the root directory of the repository 
### 1. for default run:
   1. series id : LAUCT395688200000003,LAUCT394713800000003,LAUCT485235600000003,LAUCN271410000000003,LAUCN420410000000003
   2. startyear = 2019 
   3. endyear = 2021
```
python scheduler.py
```

### 2. to run specific step use the specific flags to skip the unwanted step:
`--skip_raw_data` or `--skip_mirror_table`
```
python scheduler.py --skip_raw_data
```
```
python scheduler.py --skip_mirror_table
```

### 3. for specific series ids and years enter:


```
python scheduler.py --date_period 2018 2020 --series_id LAUCT395688200000003,LAUCT394713800000003
```

## running test
To run tests run the `pipelines/unemployment_etl/tests` dir, pytest will run the tests that are testing each step separately 

## what have could be done better
1. using infrastructure methodology to create db instead of applicative flow.
2. more unit tests, e2e test
3. scheduler better to be a abstraction to run specific pipelines for pipes to come currently running only one.
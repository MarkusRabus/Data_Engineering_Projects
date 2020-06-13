# Build a Data Lake with Spark


## Introduction

The analytics team at Sparkify is interested in understanding what songs users are listening to. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we create an ETL pipeline for a data lake hosted on S3. We will load the data from S3, process the data into analytics tables using Spark, and load them back into S3. We deploy this Spark process on a cluster using AWS.


## DB structure

The database uses the PostgreSQL as the back-end database management system. The table structure follows the star scheme.

### Fact Table

1. **songplays**
    records in log data associated with song plays i.e. records with page NextSong
    *Column names:* songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

1. **users**
    users in the app
    *Column names:* user_id, first_name, last_name, gender, level
    
2. **songs**
    songs in music database
    *Column names:* song_id, title, artist_id, year, duration
    
3. **artists**
    artists in music database
    *Column names:* artist_id, name, location, latitude, longitude
   
4. **time**
    timestamps of records in songplays broken down into specific units
    *Column names:* start_time, hour, day, week, month, year, weekday


## Repository file structure

In the data folder, example JSON files are presented. These files can be ingested into the Sparki database.

1. `DataLake_S3.ipynb`
    Jupyter notebook, which we can run on a EMR cluster to test our ETL pipeline. 
    
4. `etl.py` 
    Python script to read and process the files from *song_data* and *log_data*. Script also loads data into your tables. This is the ETL pipeline.

5. `dl.cfg` 
    Contains AWS credentials.

6. `README.md`
    This file. Provides discussion on your project. 
    



## Usage to run pipeline with Jupyter notebook on a AWS Spark cluster:

1. Create AWS Spark Cluster with the following hardware: `m5.xlarge`. Release `emr-5.29.0` and the following applications `Spark: Spark 2.4.4 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.2`

2. Run the Jupyter notebook `DataLake_s3.ipynb`. (No credentials neccessary, we are working in the cloud already.)


## Usage run pipeline on local Spark cluster:

1. Save your credentials in `dl.cfg`.

2. Run `python etl.py`


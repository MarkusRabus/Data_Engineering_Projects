# Build a Cloud Data Warehouse in AWS Cloud


## Introduction

The analytics team at Sparkify is interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in an Amazon S3 bucket.

In this project a database on a Amazon Redshift cluster is created. The tables are designed to optimize queries on song play analysis. An ETL pipeline was written so the the analytics team from Sparkify can easily query the database and compare the results with their expected results.


## DB structure

The database uses the PostgreSQL as the back-end database management system and is hosted on Redshift. The table structure follows the star scheme.

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


1. `launch_redshift.py`
    Launches a Redshift cluster and creates a IAM role for the data warehouse. Configuration of the cluster, database and credentials are defined in `dwh.cfg` 

2. `status_redshift.py`
    Check and print status of Redshift cluster. If cluster becomes available, the script also writes the redshift cluster endpoint and IAM role ARN that gives access to Redshift to database in configuration file.

2. `delete_redshift.py`
    Delete Redshift cluster and IAM role.
    
2. `create_table.py`
    Python script connects to AWS redshift and first drops the tables and then creates new ones. Run this file to reset the tables before each time you run the ETL scripts. 
    
4. `etl.py` 
    Python script to read and process the files from *song_data* and *log_data* on S3. Script also loads data into tables. This is the ETL pipeline.

5. `sql_queries.py` 
    Contains all SQL queries, and is imported into the files above.

6. `check_S3_objects` 
    Print objects in a S3 bucket on AWS.

7. `README.md`
    This file. Provides discussion on project. 

8. `dwh.cfg`
    Configuration file in the home directory. Saves credentials and Redshift cluster setup configuration. Files should have the format shown below. `key`, `secret` and `db_password` should be added.

{
[AWS]
key =
secret =

[DWH]
dwh_cluster_type = multi-node
dwh_num_nodes = 4
dwh_node_type = dc2.large
dwh_iam_role_name = dwhRole
dwh_cluster_identifier = dwhCluster

[CLUSTER]
host = ''
db_name = project3db
db_user = project3_user
db_password = ''
db_port = 5439

[IAM_ROLE]
arn = ''

[S3]
log_data = 's3://udacity-dend/log_data'
log_jsonpath = 's3://udacity-dend/log_json_path.json'
song_data = 's3://udacity-dend/song_data'
}

## Prequisites:

1. Python 3.5 or higher

2. pandas 

3. psycopg2

4. boto3



## Usage:

1. Run `python launch_redshift`. This will create a new Redshift cluster and IAM role on AWS.

2. Run `python check_status.py`. This will check the status of the Redshift cluster on AWS and if available write the redshift cluster endpoint and IAM role ARN hat gives access to Redshift to database in the configuration file..

3. Run `python create_table.py`. This will drop existing tables and create new ones.

4. Run `python etl.py`. This popluates the tables. It will read the corresponding JSON files and insert the data.

5. Query the database using PostgresSQL commands on AWS: 
	- Find `Amazon Redshift` service. 
	- In Redshift service use the `Query editor` and select `public` schema to execute queries. (See example query below.)

6. Run `python delete_redshift`. This will delete the Redshift cluster and IAM role on AWS.

## Query examples:

```
SELECT songplays.user_id, SUM(songs.duration) AS durationsum
FROM (songplays JOIN songs ON songplays.song_ID=songs.song_ID 
JOIN users ON songplays.user_id=users.user_id) 
GROUP BY songplays.user_id ORDER BY durationsum DESC;
```

This query returns the *user_id* and the sum of the duration for each user and orders the total duration descending.

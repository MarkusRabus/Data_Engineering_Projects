# Apache Cassandra Database


## Introduction

Sparkify wants to analyze the data on songs and user activity on their new 
music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. 
Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of 
CSV files on user activity on the app.

In this project I modelled the data an Apache Cassandra database which can create queries on song play data to answer questions, 



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

1. `test.ipynb`
    Jupyter notebook, which shows query examples. Current version displays the first few rows of each table. Used to check the database. 
    
2. `create_table.py`
    Python script connects to the database and first drops the tables and then creates new ones. Run this file to reset the  tables before each time you run the ETL scripts. 
    
3. `etl.ipynb`
    Jupiter notebook, reads and processes a single file from *song_data* and *log_data* directories. Loads the data into your tables. This Jupiter notebook can be used for a better understanding of the ETL pipeline.
    
4. `etl.py` 
    Python script to read and process the files from *song_data* and *log_data*. Script also loads data into your tables. This is the ETL pipeline.

5. `sql_queries.py` 
    Contains all sql queries, and is imported into the last three files above.

6. `README.md`
    This file. Provides discussion on your project. 
    

## Prequisites:

1. PostgreSQL

2. Python 3.5 or higher

3. pandas and psycopg2

4. Jupyter (optinal to run the jupyter notebooks)



## Usage:

1. Run `python create_table.py`. This will drop existing tables and create new ones.

2. Run `python etl.py`. This popluates the tables. It will read the corresponding JSON files and insert the data.

3. Query the database using PostgresSQL commands. Run `jupyter test.ipynb` to see examples.



## Query examples:

```
SELECT songplays.user_id, SUM(songs.duration) FROM (songplays JOIN songs ON songplays.song_ID=songs.song_ID \
            JOIN users ON songplays.user_id=users.user_id) GROUP BY songplays.user_id;
```

This query returns the *user_id* and the sum of the song duration.


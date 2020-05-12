import configparser
import os

####
# CONFIG
####
config = configparser.ConfigParser()
config.read_file(open( os.path.expanduser('~/dwh.cfg') ))


####
# DROP TABLES
####
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


####
# CREATE TABLES
####
staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events
        (artist text,
        auth text,
        firstName text,
        gender varchar(1),
        itemInSession int,
        lastName text,
        length numeric,
        level varchar,
        location text,
        method varchar(3),
        page varchar,
        registration double precision,
        sessionId int,
        song text,
        status int,
        ts bigint,
        userAgent text,
        userId int)
        """)


staging_songs_table_create= ("""CREATE TABLE IF NOT EXISTS staging_songs
        (num_songs int, 
        artist_id varchar, 
        artist_latitude numeric, 
        artist_longitude numeric, 
        artist_location varchar, 
        artist_name varchar, 
        song_id varchar, 
        title varchar, 
        duration numeric, 
        year int)
        """)


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays 
        (songplay_id int IDENTITY(0,1) PRIMARY KEY, 
        start_time bigint NOT NULL, 
        user_id int NOT NULL, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int, 
        location varchar, 
        user_agent text);
        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users 
        (user_id int PRIMARY KEY, 
        first_name varchar, 
        last_name varchar, 
        gender varchar(1), 
        level varchar);
        """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs 
        (song_id varchar PRIMARY KEY, 
        title text, 
        artist_id varchar, 
        year int, 
        duration numeric);
        """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists 
        (artist_id varchar PRIMARY KEY, 
        name varchar, 
        location text, 
        latitude numeric, 
        longitude numeric);
        """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
        (start_time TIMESTAMP PRIMARY KEY, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL);
        """)


####
# STAGING TABLES
####
staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    json {}
    compupdate off
    region 'us-west-2';
    """).format(config.get("S3","LOG_DATA"), 
            config.get("IAM_ROLE", "ARN"), 
            config.get("S3", "LOG_JSONPATH") )

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto' truncatecolumns
    compupdate off
    region 'us-west-2';
    """).format( config.get("S3","SONG_DATA"), 
            config.get("IAM_ROLE", "ARN") )


####
# FINAL TABLES
####
songplay_table_insert = ("""INSERT INTO songplays 
        (start_time, user_id, level, song_id, artist_id, 
        session_id, location, user_agent) 
        SELECT  staging_events.ts,
                staging_events.userId,
                staging_events.level,
                staging_songs.song_id,
                staging_songs.artist_id,
                staging_events.sessionId,
                staging_events.location,
                staging_events.userAgent
        FROM staging_events 
        JOIN staging_songs 
                ON (staging_events.artist = staging_songs.artist_name)
                AND (staging_events.song = staging_songs.title)
                AND (staging_events.length = staging_songs.duration)
                WHERE staging_events.page = 'NextSong';
        """)

user_table_insert = ("""INSERT INTO users 
        (user_id, first_name, last_name, gender, level) 
        SELECT  staging_events.userId,
                staging_events.firstName,
                staging_events.lastName,
                staging_events.gender,
                staging_events.level
        FROM staging_events
        WHERE userId is not NULL
        """)

song_table_insert = ("""INSERT INTO songs 
        (song_id, title, artist_id, year, duration)
        SELECT  staging_songs.song_id,
                staging_songs.title,
                staging_songs.artist_id,
                staging_songs.year,
                staging_songs.duration
        FROM staging_songs;
        """)

artist_table_insert = ("""INSERT INTO artists 
        (artist_id, name, location, latitude, longitude)
        SELECT  staging_songs.artist_id,
                staging_songs.artist_name,
                staging_songs.artist_location,
                staging_songs.artist_latitude,
                staging_songs.artist_longitude
        FROM staging_songs;
        """)

time_table_insert = ("""INSERT INTO time 
        (start_time, hour, day, week, month, year, weekday)
        SELECT ts.start_time,
        EXTRACT (HOUR FROM ts.start_time), EXTRACT (DAY FROM ts.start_time),
        EXTRACT (WEEK FROM ts.start_time), EXTRACT (MONTH FROM ts.start_time),
        EXTRACT (YEAR FROM ts.start_time), EXTRACT (WEEKDAY FROM ts.start_time) FROM
        (SELECT TIMESTAMP 'epoch' + songplays.start_time/1000 *INTERVAL '1 second' 
        AS start_time FROM songplays) ts;
        """)


####
# QUERY LISTS
####
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

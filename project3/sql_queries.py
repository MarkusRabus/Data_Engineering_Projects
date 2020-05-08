import configparser
import os

# CONFIG
config = configparser.ConfigParser()
config.read_file(open( os.path.expanduser('~/dwh.cfg') ))

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

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
        (songplay_id int PRIMARY KEY, 
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
        (start_time bigint PRIMARY KEY, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL);
        """)

# STAGING TABLES

staging_events_copy = ("""
	copy staging_events from '{}' 
	credentials 'aws_iam_role={}'
	json {}
	compudate off
	gzip region 'us-west-2';
	""").format(config.get("S3","LOG_DATA"), 
			config.get("IAM_ROLE", "ARN"), 
			config.get("S3", "LOG_JSONPATH") )

staging_songs_copy = ("""
	copy staging_songs from '{}' 
	credentials 'aws_iam_role={}'
	json 'auto' truncatecolumns
	compudate off
	region 'us-west-2';
	""").format( config.get("S3","SONG_DATA"), 
			config.get("IAM_ROLE", "ARN") )

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays 
        (start_time, user_id, level, song_id, artist_id, 
        session_id, location, user_agent) VALUES 
        (%s, %s, %s, %s, %s, %s, %s, %s)
        """)

user_table_insert = ("""INSERT INTO users 
        (user_id, first_name, last_name, gender, level) VALUES 
        (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id)
        DO UPDATE
        SET level = EXCLUDED.level;
        """)

song_table_insert = ("""INSERT INTO songs 
        (song_id, title, artist_id, year, duration) VALUES 
        (%s, %s, %s, %s, %s)
        ON CONFLICT (song_id)
        DO NOTHING;
        """)

artist_table_insert = ("""INSERT INTO artists 
        (artist_id, name, location, latitude, longitude) VALUES 
        (%s, %s, %s, %s, %s)
        ON CONFLICT (artist_id)
        DO NOTHING;
        """)

time_table_insert = ("""INSERT INTO time 
        (start_time, hour, day, week, month, year, weekday) VALUES 
        (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (start_time)
        DO NOTHING;
        """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
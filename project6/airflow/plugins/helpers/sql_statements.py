class SqlQueriesTables:
    
    STAGING_EVENTS_DROP_SQL = ("""DROP TABLE IF EXISTS staging_events""")
    STAGING_SONGS_DROP_SQL = ("""DROP TABLE IF EXISTS staging_songs""")
    SONGPLAYS_DROP_SQL = ("""DROP TABLE IF EXISTS songplays""")
    USERS_DROP_SQL = ("""DROP TABLE IF EXISTS users""")
    SONGS_DROP_SQL = ("""DROP TABLE IF EXISTS songs""")
    ARTISTS_DROP_SQL = ("""DROP TABLE IF EXISTS artists""")
    TIME_DROP_SQL = ("""DROP TABLE IF EXISTS time""")


    CREATE_ARTISTS_TABLE_SQL = ("""
    CREATE TABLE public.artists (
    	artist_id varchar(256) NOT NULL,
    	name varchar(256),
    	location varchar(256),
    	latitude numeric(18,0),
    	longitude numeric(18,0)
    );
    """)
    
    CREATE_TIME_TABLE_SQL = ("""
    CREATE TABLE IF NOT EXISTS time 
        (start_time TIMESTAMP PRIMARY KEY, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL);
    """)

    CREATE_SONGPLAYS_TABLE_SQL = ("""
    CREATE TABLE public.songplays (
    	songplay_id int IDENTITY(0,1) PRIMARY KEY, 
    	start_time TIMESTAMP NOT NULL,
    	user_id int4 NOT NULL,
    	level varchar(256),
    	song_id varchar(256),
    	artist_id varchar(256),
    	session_id int4,
    	location varchar(256),
    	user_agent varchar(256)
    );
    """)

    CREATE_SONGS_TABLE_SQL = ("""
    CREATE TABLE public.songs (
    	song_id varchar(256) NOT NULL,
    	title varchar(256),
    	artist_id varchar(256),
    	"year" int4,
    	duration numeric(18,0),
    	CONSTRAINT songs_pkey PRIMARY KEY (song_id)
    );
    """)

    CREATE_STAGING_EVENTS_TABLE_SQL = ("""
    CREATE TABLE public.staging_events (
    	artist text,
        auth text,
        firstName text,
        gender varchar(1),
        itemInSession int,
        lastName text,
        length float,
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
        userId int
    );
    """)

    CREATE_STAGING_SONGS_TABLE_SQL = ("""
    CREATE TABLE public.staging_songs (
    	num_songs int, 
        artist_id varchar, 
        artist_latitude float, 
        artist_longitude float, 
        artist_location varchar, 
        artist_name varchar, 
        song_id varchar, 
        title varchar, 
        duration float, 
        year int
    );
    """)

    CREATE_USERS_TABLE_SQL = ("""
    CREATE TABLE public.users (
    	user_id int4 NOT NULL,
    	first_name varchar(256),
    	last_name varchar(256),
    	gender varchar(256),
    	"level" varchar(256),
    	CONSTRAINT users_pkey PRIMARY KEY (user_id)
    );
    """)





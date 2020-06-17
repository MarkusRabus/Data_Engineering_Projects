import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, \
        weekofyear, dayofweek, date_format
from pyspark.sql.types import StructType, StructField as Fld, DoubleType as Dbl, \
        StringType as Str, IntegerType as Int, DateType as Date, StringType as Str, \
        LongType as Lgt, TimestampType as Tme

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']




def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config('spark.sql.session.timeZone', 'UTC') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    
    Function to load the sond-data JSON files, create the corresponding tables and
    save the tables back as PARQUET.
    
    Creates and and saves the tables:
    - songs
    - artists
    
    INPUT:
    - pyspark session
    - input data, S3 bucket of the data
    - ouput data, S3 bucket to save the PARQUET files.
    
    '''
    
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/A/A/*/*.json") #partial data set
    #song_data = os.path.join(input_data, "song_data/*/*/*/*.json") #full data set
    
    # read song data file
    df = spark.read.json( song_data, schema=create_songSchema() )

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration') \
                    .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite') \
                    .parquet( os.path.join(output_data, "songs.parquet") )

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'), 
                            col('artist_name').alias('name'), 
                            col('artist_location').alias('location'), 
                            col('artist_latitude').alias('latitude'),
                            col('artist_longitude').alias('longitude'),) \
                            .dropDuplicates()
    
    # write artists table to parquet files partitioned by name
    artists_table.write.mode('overwrite') \
                        .parquet( os.path.join(output_data, "artists.parquet") )


def process_log_data(spark, input_data, output_data):
    '''
    
    Function to load the sond-data JSON files, create the corresponding tables and
    save the tables back as PARQUET.
    
    Creates and and saves the tables:
    - users
    - time
    - songplays
    
    INPUT:
    - pyspark session
    - input data, S3 bucket of the data
    - ouput data, S3 bucket to save the PARQUET files.
    
    '''
    
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data, schema=create_logSchema())
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = df.select(col('userId').alias('user_id'), 
                            col('firstName').alias('first_name'), 
                            col('lastName').alias('last_name'), 
                            col('gender'), 
                            col('level')).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite') \
                        .parquet( os.path.join(output_data, "users.parquet") )

    # create timestamp column from original timestamp column
    get_timestamp = udf( lambda x: x/1000.0 )
    df = df.withColumn( 'start_time', get_timestamp('ts').cast(Tme()) )
    
    # create datetime column from original timestamp column
    get_datetime = udf( lambda x: x/1000.0 )
    df = df.withColumn( 'date_time', get_datetime('ts').cast(Tme()) )
    
    # extract columns to create time table
    time_table = df.select(col('start_time').alias('start_time'), 
                        hour(df.start_time).alias('hour'), 
                        dayofmonth(df.start_time).alias('day'), 
                        weekofyear(df.start_time).alias('week'),
                        month(df.start_time).alias('month'),
                        year(df.start_time).alias('year'),
                        dayofweek(df.start_time).alias('weekday')) \
                        .dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite') \
                    .parquet( os.path.join(output_data, "time.parquet") )

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/A/A/A/*.json") #partial data set
    #song_data = os.path.join(input_data, "song_data/*/*/*/*.json") #full data set
    song_df = spark.read.json( song_data, schema=create_songSchema() )

    # extract columns from joined song and log datasets to create songplays table
    log_join_song_df = df.join(song_df, (song_df.artist_name == df.artist) \
                               & (song_df.title == df.song))
    
    songplays_table =  log_join_song_df.select(col('start_time'), 
                        col('userId').alias('user_id'), 
                        col('level'), 
                        col('song_id'), 
                        col('artist_id'),
                        col('sessionId').alias('session_id'),
                        col('location'),
                        col('userAgent').alias('user_agent'), 
                        month(col('start_time')).alias('month'),
                        year(col('start_time')).alias('year'),) \
                        .withColumn('songplay_id', monotonically_increasing_id()) \
                        .filter("page = 'NextSong'").dropDuplicates()



    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month") \
                        .mode('overwrite') \
                        .parquet( os.path.join(output_data, "songplays.parquet") )

    
def create_songSchema():
    '''
    This function creates and returns the song schema of the JSON file.
    
    '''
    songSchema = StructType([
        Fld("artist_id",       Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_longitude",Dbl()),
        Fld("artist_name",     Str()),
        Fld("duration",        Dbl()),
        Fld("num_songs",       Int()),
        Fld("song_id",         Str()),
        Fld("title",           Str()),
        Fld("year",            Int())
        ])
    return songSchema

def create_logSchema():
    '''
    This function creates and returns the log schema of the JSON file
    
    '''
    logschema = StructType([
        Fld('artist',          Str()),
        Fld('auth',            Str()),
        Fld('firstName',       Str()),
        Fld('gender',          Str()),
        Fld('itemInSession',   Int()),
        Fld('lastName',        Str()),
        Fld('length',          Dbl()),
        Fld('level',           Str()),
        Fld('location',        Str()),
        Fld('method',          Str()),
        Fld('page',            Str()),
        Fld('registration',    Dbl()),
        Fld('sessionId',       Int()),
        Fld('song',            Str()),
        Fld('status',          Int()),
        Fld('ts',              Lgt()),
        Fld('userAgent',       Str()),
        Fld('userId',          Int())
        ]) 
    return logschema
    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

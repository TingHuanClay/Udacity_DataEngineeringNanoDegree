import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Description:
        Retrieve an existed Spark session or create a new one

    Arguments:
        None

    Returns:
        spark: Spark session
    """
    print("1. create_spark_session [START]")
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    print("\n1. create_spark_session [Finished]")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description:
        Loads song_data from S3 and extracting the songs and artist data
        and loaded them back to S3 bbucket

    Arguments:
        spark       : Spark Session
        input_data  : location of song_data json files with the songs metadata
        output_data : S3 bucket were dimensional tables in parquet format will be stored

    Returns:
        None
    """
    print("\n2. process_song_data from {} [START]".format(input_data))
    
    # get filepath to song data file
    # Song data: s3://udacity-dend/song_data
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    print("Step 1: Loading data to Spark")
    df = spark.read.json(song_data)
    
    
    df.createOrReplaceTempView("song_data_table")
    
    print("Step 2: Processing data for song_table and artist_table")
    # extract columns to create songs table
    songs_table = (
        df.select(
            'song_id',
            'title',
            'artist_id',
            'year',
            'duration'
        ).distinct()
    )
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/', mode="overwrite")
    
    print("  Step 2-1: songs_table is written back to {}.".format(output_data + 'songs/'))
    

    # extract columns to create artists table
    artists_fields = ["artist_id",
                      "artist_name as name",
                      "artist_location as location",
                      "artist_latitude as latitude",
                      "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/', mode="overwrite")
    
    print("  Step 2-2: artists_table is written back to {}.".format(output_data + 'artists/') )
    
    print("[Finished] process_song_data FROM {} TO {}".format(input_data, output_data))


def process_log_data(spark, input_data, output_data):
    """
    Description:
        Loads log_data from S3 and process the event_log via extracting the data with the 'NextSong' event.
        and loaded them back to S3 bbucket

    Arguments:
        spark       : Spark Session
        input_data  : location of log_data json files with the songs metadata
        output_data : S3 bucket were dimensional tables in parquet format will be stored

    Returns:
        None
    """

    print("\n3. process_log_data from {} [START]".format(input_data))
    
    # get filepath to log data file
    # Log data: s3://udacity-dend/log_data
    print("Step 1: Loading data to Spark")
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file 
    df = spark.read.json(log_data)
    
    
    print("Step 2: Processing data for user_table, time_table and songplays_table")
    # filter by actions for song plays
    # focus on 'NextSong' event
    df = df.filter(df.page == 'NextSong')
    
    # created log view to write SQL Queries
    df.createOrReplaceTempView("log_data_table")
    
    # extract columns for users table
    users_table = spark.sql("""
                            SELECT DISTINCT 
                                   userId as user_id,
                                   firstName as first_name,
                                   lastName as last_name,
                                   gender as gender,
                                   level as level
                              FROM log_data_table
                             WHERE userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", mode="overwrite")
    print("  Step 2-1: users_table is written back to {}.".format(output_data + "users/"))

    
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT 
                                   logDT.start_time,
                                   hour(logDT.start_time) as hour,
                                   dayofmonth(logDT.start_time) as day,
                                   weekofyear(logDT.start_time) as week,
                                   month(logDT.start_time) as month,
                                   year(logDT.start_time) as year,
                                   dayofweek(logDT.start_time) as weekday
                            FROM (
                                 SELECT to_timestamp(ts/1000) as start_time
                                   FROM log_data_table
                                  WHERE ts IS NOT NULL
                            ) logDT
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + 'time/', mode="overwrite")
    print("  Step 2-2: time_table is written back to {}.", output_data + "time/")


    """
    extract columns from joined song and log datasets to create songplays table
    Noticed: since the there is NO song_id & artist_Id in log_data
             we can only join the table via artist_name & son_title as below
    """
    songplays_table = spark.sql("""
        SELECT 
        	   monotonically_increasing_id() as songplay_id,
        	   to_timestamp(logDT.ts/1000) as start_time,
        	   month(to_timestamp(logDT.ts/1000)) as month,
        	   year(to_timestamp(logDT.ts/1000)) as year,
        	   logDT.userId as user_id,
        	   logDT.level as level,
        	   songDT.song_id as song_id,
        	   songDT.artist_id as artist_id,
        	   logDT.sessionId as session_id,
        	   logDT.location as location,
        	   logDT.userAgent as user_agent
          FROM log_data_table logDT
          JOIN song_data_table songDT 
            ON logDT.artist = songDT.artist_name 
           AND logDT.song = songDT.title
    """)
                    
    songplays_table.write.parquet(output_data + "songplays/", mode="overwrite")
    print("  Step 2-3: songplays_table is written back to {}.", output_data + "songplays/")
    
    print("[Finished] process_log_data FROM {} TO {}".format(input_data, output_data))

    
def main():
    spark = create_spark_session()
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-udacity-dend/"
    

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

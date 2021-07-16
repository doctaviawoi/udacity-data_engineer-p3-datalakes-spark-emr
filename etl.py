import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *



config = configparser.ConfigParser()
config.read('dl.cfg')

aws_profile = 'udacity_dend_p3_datalake_spark_project'
os.environ['AWS_ACCESS_KEY_ID']=config.get(aws_profile, 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get(aws_profile, 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''
    Start Spark session and load config JAR package of hadoop-aws
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Loads song data from input_data path, creates the the tables below and
    writes each table to parquet file in output_data path.
        - song_table
            Columns: song_id, title,artist_id, year and duration.
            Partition by: year and artist_id.
        - artists_table
            Columns: artist_id, name, location, latitude, longitude.

    Parameters:
    spark: existing Spark session.
    input_data (str): path to the song data files.
    output_data (str): destination path to write tables.
    '''

    # get filepath to song data file
    song_data = input_data

    # read song data file
    print("          - reading song data files....")
    df = spark.read.json(song_data)
    print("          - song data files loaded!")

    # extract columns to create songs table
    print("          - processing songs_table....")
    songs_fields = ["song_id",\
                    "title",\
                    "artist_id",\
                    "year",\
                    "duration"]

    songs_table = df.select(songs_fields)\
                    .where("song_id != 'None' or artist_id != 'None'")\
                    .dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id")\
                    .parquet(os.path.join(output_data, 'songs/'))

    print("          - songs_table written to S3!")

    # extract columns to create artists table
    print("          - processing artists_table....")

    artists_fields = ["artist_id",\
                    "artist_name as name",\
                    "artist_location as location",\
                    "artist_latitude as latitude",\
                    "artist_longitude as longitude"]

    artists_table = df.selectExpr(artists_fields)\
                        .where("artist_id != 'None'")\
                        .dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite")\
                .parquet(os.path.join(output_data, "artists/"))

    print("          - artists_table written to S3!")


def process_log_data(spark, input_data, output_data):
    '''
    Loads log data from input_data path, creates the the tables below and
    writes each table to parquet file in output_data path.
        - users_table
            Columns: user_id, firts_name, last_name, gender, level.
        - time_table
            Columns: unix_timestamp_ms, start_time, hour, day, week, month,
                     year, weekday.
            Partition by: year and month.
        - songplays_table
            Columns: start_timestamp, year, month, user_id, level, song_id,
                     artist_id, session_id, location, user_agent.
            Partition by: year and month.

    Parameters:
    spark: existing Spark session.
    input_data (str): path to the log data files.
    output_data (str): destination path to write tables.
    '''

    # get filepath to log data file
    log_data = input_data

    # read log data file
    print("          - reading log data files....")
    df = spark.read.json(log_data)
    print("          - log data files loaded!")

    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table
    print("          - processing users_table....")

    users_fields = ["userId as user_id", \
                   "firstname as first_name", \
                   "lastname as last_name", \
                   "gender", \
                   "level"]

    users_table = df.selectExpr(users_fields)\
                    .where("user_id != 'None'")\
                    .dropDuplicates()

    # write users table to parquet files
    users_table = users_table.write.mode("overwrite")\
                    .parquet(os.path.join(output_data, "users/"))

    print("          - users_table written to S3!")

    # create timestamp column from original timestamp column
    print("          - processing time_table....")

    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0, tz=None)\
                                   .strftime('%Y-%m-%d %H:%M:%S'))

    df = df.withColumn("timestamp", \
                       get_timestamp(df.ts))

    # create datetime column from original timestamp column
    df = df.withColumn("datetime", to_timestamp(col("timestamp")))


    # extract columns to create time table
    time_table = df.selectExpr("ts as unix_timestamp_ms",\
                                "datetime as start_time",\
                                "hour(datetime) as hour",\
                               "dayofmonth(datetime) as day",\
                               "weekofyear(datetime) as week",\
                               "month(datetime) as month",\
                               "year(datetime) as year",\
                               "date_format(datetime, 'u') as weekday"\
                              ).dropDuplicates()


    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month")\
                    .parquet(os.path.join(output_data, 'time/'))

    print("          - time_table written to S3!")

    # read in song data to use for songplays table
    print("          - processing songplays_table....")

    song_df = spark.read.parquet(os.path.join(output_data, 'songs/'))

    # drop year column in song_df to avoid ambiguity (with year column in time_table)
    song_df = song_df.drop("year")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df,\
                              on = [df['song']==song_df['title'], df['length']==song_df['duration']],\
                              how = 'inner')\
                        .join(time_table, on = df['ts']==time_table['unix_timestamp_ms'], how = 'inner')\
                        .selectExpr("start_time as start_timestamp",\
                                    "year",\
                                    "month",\
                                    "userId as user_id",\
                                    "level",\
                                    "song_id",\
                                    "artist_id",\
                                    "sessionId as session_id",\
                                    "location",\
                                    "userAgent as user_agent")\
                        .dropDuplicates()


    # add row index with column name of songplay_id to songplay_table
    songplays_table.createOrReplaceTempView("songplays")

    songplays_table = spark.sql("""
                                    SELECT CAST(ROW_NUMBER() OVER(ORDER BY songplays.start_timestamp ASC) AS BIGINT) songplay_id, *
                                    FROM songplays
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month")\
                         .parquet(os.path.join(output_data, 'songplays/'))

    print("          - songplays_table written to S3!")


def main():
    print("Status: creating Spark session...")
    spark = create_spark_session()
    print("Status: Spark session created!")
    input_data_song = "s3a://udacity-dend/song_data/*/*/*/*.json"
    input_data_log = "s3a://udacity-dend/log_data/*/*/*.json"
    output_data = "s3a://udacity-dend-p3-datalakes-spark/output/"

    print("Status: processing song data files...")
    process_song_data(spark, input_data_song, output_data)
    print("Status: song data files processed and written to S3 bucket!")

    print("Status: processing log data files...")
    process_log_data(spark, input_data_log, output_data)
    print("Status: log data files processed and written to S3 bucket!")

if __name__ == "__main__":
    main()

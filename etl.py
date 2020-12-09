import configparser
from datetime import datetime
import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window


import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

# Extarct data from s3 file from s3 bucket prkify/data/song_data
# laod it in to Song, Artist Tables EL

def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    column = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df.selectExpr(*column).dropDuplicates()
    # create a song table
    songs_table.createOrReplaceTempView('songs')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite') \
        .partitionBy("year", "artist_id") \
        .parquet(os.path.join(output_data + 'songs/'))

    # extract columns to create artists table
    column = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude ',
              'artist_longitude']
    artists_table = df.selectExpr(*column).dropDuplicates()

    # create artist table

    # write artists table to parquet files
    artists_table.write.mode('overwrite') \
        .parquet(os.path.join(output_data + 'artists/'))


# Extarct the log file from s3 bucket prkify/data/log-data
#load the data into table and transform the data --- ELT

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log-data/*.json'


    # read log data file
    df = spark.read.json(log_data)
        

    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
  

    # # extract columns for users table
    column = ['artist', 'firstName', 'lastName', 'gender', 'level']
    artists_table = df.selectExpr(*column).dropDuplicates()

    # # write users table to parquet files
    artists_table.write.mode('overwrite') \
        .parquet(os.path.join(output_data + 'users_log/'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: (
        datetime.fromtimestamp(x / 1000.0)), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    df = df.withColumn('datetime', df.timestamp)
    

    # extract columns to create time table
    time_table =df.select('timestamp') \
        .withColumn('hour', hour('timestamp')) \
        .withColumn('day', dayofmonth('timestamp')) \
        .withColumn('week', weekofyear('timestamp')) \
        .withColumn('month', month('timestamp')) \
        .withColumn('year', year('timestamp'))

    
    time_table.createOrReplaceTempView('time')
    # print(time_table.limit(2).toPandas())
    # sys.exit("stop hereeeeeeee")

    

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite') \
        .partitionBy('year', 'month') \
        .parquet(os.path.join(output_data + 'time/'))
    

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    #extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, song_df.artist_name == df.artist , how='left') \
         .join(time_table, on=['timestamp'], how='inner') \
         .withColumn('songplays_id', F.row_number().over(Window.orderBy(F.monotonically_increasing_id()))) \
         .select('songplays_id', 'userId', 'timestamp', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', time_table.year ,'month').dropDuplicates()
    
    #print(songplays_table.limit(2).toPandas())
    #sys.exit("stop hereeeeeeee")

    #write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite') \
    .partitionBy('year','month') \
    .parquet(os.path.join(output_data + 'songplays/'))
    
    


def main():
    spark = create_spark_session()
    input_data = "s3://sprkify/data/"
    output_data = "s3://sprkify/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

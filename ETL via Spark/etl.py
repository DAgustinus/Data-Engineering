import configparser
from datetime import datetime
import os
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth as day, hour, weekofyear as week, dayofweek as weekday, date_format
from pyspark.sql.types import StructType as ST, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, StringType as Str, LongType as Lng, TimestampType as Tst


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

data_location=config['LOCATION']['S3_DATA']
output_location=config['LOCATION']['S3_OUTPUT_DATA']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    print('%%%%% Spark has been acquired')
    return spark


def process_song_data(spark, input_data, output_data):
    
    print('%%%%% Starting up the SONG data process')
    
    # get filepath to song data file
    song_data = 'song_data/A/*/*/*.json'
    
    # setting up the schema for the data that we're about to pull
    songSchema = ST([
                    Fld("num_songs",Int()),
                    Fld("artist_id",Str()),
                    Fld("artist_latitude",Dbl()),
                    Fld("artist_longitude",Dbl()),
                    Fld("artist_location",Str()),
                    Fld("artist_name",Str()),
                    Fld("song_id",Str()),
                    Fld("title",Str()),
                    Fld("duration",Dbl()),
                    Fld("year",Int())
                    ])
    
    # read song data file - Uncomment the line below to download from S3, otherwise line 30 will access the data locally
    #df = spark.read.json(input_data + song_data)
    raw_song_df = spark.read.json(input_data + song_data, songSchema)
        
    # extract columns to create songs table
    songs_table = raw_song_df.select(raw_song_df.song_id, \
                                 raw_song_df.title, \
                                 raw_song_df.artist_id, \
                                 raw_song_df.year.cast(Int()), \
                                 raw_song_df.duration.cast(Dbl()))
        
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data + 'songs')

    print('%%%%% Songs table has been created and written to the S3 Bucket')

    # extract columns to create artists table
    artists_table = raw_song_df.select(raw_song_df.artist_id , \
                                  raw_song_df.artist_latitude.alias('latitude'), \
                                  raw_song_df.artist_location.alias('location'), \
                                  raw_song_df.artist_longitude.alias('longitude'), \
                                  raw_song_df.artist_name.alias('name')).dropDuplicates(['artist_id','name'])
        
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artist')
    
    print('%%%%% Artists table has been created and written to the S3 Bucket')
    print('%%%%% SONG data has been completed and returning the raw_song_df')
    return raw_song_df


def process_log_data(spark, input_data, output_data, raw_song_df):
    
    print('%%%%% Starting up the LOG data process')
    
    # get filepath to log data file
    log_data = 'log_data'
    
    # setting up the log schema
    logSchema = ST([
                 Fld('artist', Str()),
                 Fld('auth', Str()),
                 Fld('firstName', Str()),
                 Fld('gender', Str()),
                 Fld('itemInSession', Lng()),
                 Fld('lastName', Str()),
                 Fld('length', Dbl()),
                 Fld('level', Str()),
                 Fld('location', Str()),
                 Fld('method', Str()),
                 Fld('page', Str()),
                 Fld('registration', Dbl()),
                 Fld('sessionId', Lng()),
                 Fld('song', Str()),
                 Fld('status', Lng()),
                 Fld('ts', Lng()),
                 Fld('userAgent', Str()),
                 Fld('userId', Str())
                ])
    
    # read log data file
    raw_log_df = spark.read.json(input_data + log_data, logSchema)
    # uncomment below to try to read from LOCAL
    # raw_log_df = spark.read.json('/home/workspace/data/log_data', logSchema)

    # filter by actions for song plays
    df_l = raw_log_df.where(raw_log_df.page == 'NextSong')
    
    # Join the raw_song_df and the df_l to be used later for song plays table
    # For some odd reason, the 'ts' column which we have stated as a timestamp changes to a     
    #  long type. In line 116, I've re-cast the column to Timestamp so we can convert it later
    #  for the songplays table
    j_tbl = df_l.join(raw_song_df, [raw_song_df.title == df_l.song, df_l.artist == raw_song_df.artist_name]).withColumn('ts',df_l.ts.cast(Tst()))
    
    # During this phase, we will convert the 'ts' column to a timestamp column type and convert the epoch time 
    # by using pyspark function called to_timestamp
    df_l = df_l.withColumn('ts',f.to_timestamp(df_l.ts / 1000).cast(Tst()))

    # extract columns for users table    
    users_table = df_l.select(df_l.userId.alias('user_id').cast(Int()), \
                          df_l.firstName.alias('first_name'), \
                          df_l.lastName.alias('last_name'), \
                          df_l.gender, \
                          df_l.level).dropDuplicates(['user_id'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')
    
    print('%%%%% Users table has been created and written to the S3 Bucket')
    
    # extract columns to create time table
    time_table = df_l.select(df_l.ts.alias('start_time'), \
                        hour('ts').alias('hour').cast(Int()), \
                        day('ts').alias('day').cast(Int()), \
                        week('ts').alias('week').cast(Int()), \
                        month('ts').alias('month').cast(Int()), \
                        year('ts').alias('year').cast(Int()), \
                        weekday('ts').alias('weekday')).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + 'time')

    print('%%%%% Time table has been created and written to the S3 Bucket')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = j_tbl.select(f.monotonically_increasing_id().alias('songplay_id'), \
                               j_tbl.ts.alias('start_time'), \
                               year('ts').alias('year'), \
                               month('ts').alias('month'), \
                               j_tbl.userId.alias('user_id').cast(Int()), \
                               j_tbl.level, \
                               j_tbl.song_id, \
                               j_tbl.artist_id, \
                               j_tbl.sessionId.alias('session_id').cast(Int()), \
                               j_tbl.artist_location.alias('location'), \
                               j_tbl.userAgent.alias('user_agent'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data + 'songplays')
    
    print('%%%%% Songplays table has been created and written to the S3 Bucket')

def main():
    spark = create_spark_session()
    input_data = data_location
    output_data = output_location
    
    raw_song_df = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, raw_song_df)


if __name__ == "__main__":
    main()

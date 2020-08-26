import argparse
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    # As suggested by Tran Nguyen here -
    # https://towardsdatascience.com/some-issues-when-building-an-aws-data-lake-using-spark-and-how-to-deal-with-these-issues-529ce246ba59
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2") 
   
    return spark


def process_song_data(spark, input_data, output_data, cliargs):

    # Full path to all the song data files.

    full_path = 'song_data/*/*/*/*.json'

    # Path to a subset of the data, so we have the option of using it 
    # for testing, as suggested by Tran Nguyen here -
    # https://towardsdatascience.com/some-issues-when-building-an-aws-data-lake-using-spark-and-how-to-deal-with-these-issues-529ce246ba59

    subset_path = 'song_data/A/A/A/*.json'

    # Read from argparse arguments to see if we want to load a subset of
    # the data (for testing purposes) or load all the data from s3. This 
    # option has been given because loading all the data from s3 can be
    # resource intensive.

    if cliargs.run_subset:
        song_df = spark.read.json(os.path.join(input_data, subset_path))
    else:
        song_df = spark.read.json(os.path.join(input_data, full_path))

    # create Spark SQL `songs` table
    song_df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            select distinct
                                songs.song_id,
                                songs.title,
                                songs.artist_id,
                                songs.year,
                                songs.duration
                            from songs
                            where song_id IS NOT NULL""")
 
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').parquet("s3a://sparkifytest/songs_table.parquet")

    # create Spark SQL `artists` table
    song_df.createOrReplaceTempView("artists")

    # extract columns to create artists table
    artists_table = spark.sql("""
                            select distinct
                                artists.artist_id as artist_id,
                                artists.artist_name as name,
                                artists.artist_location as location,
                                artists.artist_latitude as latitude,
                                artists.artist_longitude as longitude
                            from artists
                            where artist_id IS NOT NULL""")

    # write artists table to parquet files
    songs_table.write.mode('overwrite').parquet("s3a://sparkifytest/artists_table.parquet")


# def process_log_data(spark, input_data, output_data):

#     get filepath to log data file
#     log_data =

#     # read log data file
#     df = 
    
#     # filter by actions for song plays
#     df = 

#     # extract columns for users table    
#     artists_table = 
    
#     # write users table to parquet files
#     artists_table

#     # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df = 
    
#     # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 
    
#     # extract columns to create time table
#     time_table = 
    
#     # write time table to parquet files partitioned by year and month
#     time_table

#     # read in song data to use for songplays table
#     song_df = 

#     # extract columns from joined song and log datasets to create songplays table 
#     songplays_table = 

#     # write songplays table to parquet files partitioned by year and month
#     songplays_table


def main():
    parser = argparse.ArgumentParser(
        prog='etl.py',
        description="""ETL Script that extracts data from
            s3 buckets and loads them into Spark SQL tables
            before writing them to s3 once again as paraquet
            files.""")

    parser.add_argument(
        '-s', '--run-subset',
        action='store_true',
        help="""Load only a subset of data. This can be
        useful for testing a loading all the data from s3
        can be resource intensive.""")

    cliargs, _ = parser.parse_known_args()

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data, cliargs)    
    #process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

"""
Main ETL script that is run in Spark via `spark-submit`.
Loads the `.json` files in the `song_data` and `log_data` S3
folders into Spark and processes them using Spark SQL.

It then outputs the final tables back to S3 as partitioned
parquet files.
"""

import argparse
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():

    """
    Creates a Spark Session and returns it
    so that other functions can use it.

    Paramters:
    None

    Returns:
    A Spark Session Object
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    # Setting the MapReduce algorithm to v2, as suggested by Tran Nguyen here -
    # https://towardsdatascience.com/some-issues-when-building-an-aws-data-lake-using-spark-and-how-to-deal-with-these-issues-529ce246ba59
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2") 
   
    return spark


def process_song_data(spark, input_data, output_data, cliargs):

    """
    Loads `song_data` files from S3 into Spark, transforms them via Spark SQL 
    and then outputs them to a specified S3 output directory as parquet files.

    Creates two output tables:
    1) `songs`
    2) `artists`

    Parameters:
    - spark - The spark session that this function will use to connect to Spark.
    - input_data - The S3a path where the `song_data` directory can be found.
    - output_data - Where the parquet files will be written to.
    - cliargs - The CLI arguments passed in from argparse. These arguments allow the
                user to select whether to run a full ETL process or just test it on
                a subset of data first.

    Returns:
    None
    """

    # Full path to all the song data files.

    full_data_path = 'song_data/*/*/*/*.json'

    # Path to a subset of the data, so we have the option of using it
    # for testing, as suggested by Tran Nguyen here -
    # https://towardsdatascience.com/some-issues-when-building-an-aws-data-lake-using-spark-and-how-to-deal-with-these-issues-529ce246ba59

    subset_data_path = 'song_data/A/A/A/*.json'

    # Read from argparse arguments to see if we want to load a subset of
    # the data (for testing purposes) or load all the data from s3. This
    # option has been given because loading all the data from s3 can be
    # resource intensive.

    if cliargs.run_subset:
        song_df = spark.read.json(os.path.join(input_data, subset_data_path))
    else:
        song_df = spark.read.json(os.path.join(input_data, full_data_path))

    # create Spark SQL `songs` table
    song_df.createOrReplaceTempView("song_data")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            select
                                songs.song_id,
                                songs.title,
                                songs.artist_id,
                                songs.year,
                                songs.duration
                            from song_data songs
                            where song_id IS NOT NULL""")
 
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_table = spark.sql("""
                                select 
                                    distinct artists.artist_id as artist_id,
                                    artists.artist_name as name,
                                    artists.artist_location as location,
                                    artists.artist_latitude as latitude,
                                    artists.artist_longitude as longitude
                                from song_data artists
                                where artist_id IS NOT NULL""")

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data, cliargs):

    """
    Loads `log_data` files from S3 into Spark, transforms them via Spark SQL 
    and then outputs them to a specified S3 output directory as parquet files.

    Creates three output tables:
    1) `users`
    2) `time`
    3) `songplays`

    Parameters:
    - spark - The spark session that this function will use to connect to Spark.
    - input_data - The S3a path where the `song_data` directory can be found.
    - output_data - Where the parquet files will be written to.
    - cliargs - The CLI arguments passed in from argparse. These arguments allow the
                user to select whether to run a full ETL process or just test it on
                a subset of data first.
                
    Returns:
    None
    """

    # Full path to all the song data files.

    full_data_path = 'log_data/*/*/*.json'

    # Path to a subset of the data, so we have the option of using it 
    # for testing, as suggested by Tran Nguyen here -
    # https://towardsdatascience.com/some-issues-when-building-an-aws-data-lake-using-spark-and-how-to-deal-with-these-issues-529ce246ba59

    subset_data_path = 'log_data/2018/11/*.json'

    # Read from argparse arguments to see if we want to load a subset of
    # the data (for testing purposes) or load all the data from s3. This 
    # option has been given because loading all the data from s3 can be
    # resource intensive.

    if cliargs.run_subset:
        logs_df = spark.read.json(os.path.join(input_data, subset_data_path))
    else:
        logs_df = spark.read.json(os.path.join(input_data, full_data_path))

    # create Spark SQL `logs` table
    logs_df = logs_df.filter(logs_df.page == 'NextSong')

    logs_df.createOrReplaceTempView("log_data")
    
    # extract columns for users table    
    users_table = spark.sql("""
                            select 
                                distinct logs.userId as user_id,
                                logs.firstName as first_name,
                                logs.lastName as last_name,
                                logs.gender as gender,
                                logs.level as level
                            from log_data logs
                            where logs.userId IS NOT NULL""")
  
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    time_table = spark.sql("""
                            with time_cte as (
                                select
                                    to_timestamp(logs.ts/1000) as time
                                from log_data logs
                                where logs.ts is not null  
                            )
                            select
                                time_cte.time as start_time,
                                hour(time_cte.time) as hour,
                                dayofmonth(time_cte.time) as day,
                                weekofyear(time_cte.time) as week,
                                month(time_cte.time) as month,
                                year(time_cte.time) as year,
                                dayofweek(time_cte.time) as weekday
                            from time_cte""")

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "time/")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                    select
                                        monotonically_increasing_id() as songplay_id,
                                        to_timestamp(logs.ts/1000)) as start_time,
                                        month(to_timestamp(logs.ts/1000)) as month,
                                        year(to_timestamp(logs.ts/1000)) as year,
                                        logs.userId as user_id,
                                        logs.level as level,
                                        songs.song_id as song_id,
                                        songs.artist_id,
                                        logs.sessionId as session_id,
                                        logs.location,
                                        logs.userAgent as user_agent
                                    from log_data logs

                                    inner join song_data songs on logs.song = songs.title and
                                                                logs.artist = songs.artist_name                     

    """)
    # write songplays table to parquet files partitioned by year and month

    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "songplays/")

def main():

    """
    The main function of the ETL script. Reads command line
    arguments via argparse, to see whether the user wants to
    run the ETL process on a subset of the data, or on the
    full data set.

    Creates a Spark Session and passed it to the two main
    functions of this script:

    1) process_song_data - loads, transforms and outputs song data
    2) process_log_data - loads, transforms and outputs log data
    
    Params: 
    None

    Returns:
    None
    """
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

    input_data = 's3a://udacity-dend/'
    output_data = 's3a://sparkifytest/'

    process_song_data(spark, input_data, output_data, cliargs)    
    process_log_data(spark, input_data, output_data, cliargs)


if __name__ == "__main__":
    main()

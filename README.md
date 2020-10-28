# Introduction

A part of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027), this [ETL](https://en.wikipedia.org/wiki/Extract,_transform,_load) project looks to collect and present user activity information for a fictional music streaming service called Sparkify. To do this, data is gathered from song information and application `.json` log files (which were generated from the [Million Song Dataset](http://millionsongdataset.com/) and from [eventsim](https://github.com/Interana/eventsim) respectively and given to us).

These log files are stored in two [Amazon S3](https://aws.amazon.com/s3/) directories, and are loaded into an [Amazon EMR](https://aws.amazon.com/s3/) Spark cluster for processing. The `etl.py` script reads these files from S3, transforms them to create five different tables in Spark and writes them to partitioned parquet files in table directories on S3.

# Files
```
- the folder with song information and user log information, all in .json format
- README.md -- this file
- etl.py - the main ETL script that interacts with Spark to create the paraquet files
- dl.cfg - configuration file where database and AWS connection details need to be entered
```

## Setup

### Set up server
In order to run these Python scripts, you will first need to install Python 3 and [Apache Spark](https://spark.apache.org/downloads.html) on your server, and then install the following Python modules via [pip](https://pypi.org/project/pip/) or [anaconda](https://www.anaconda.com/products/individual):

- [pyspark](https://pypi.org/project/pyspark/) - a Pyspark adapter for Python

To install these via `pip` you can run:

`pip install pyspark`

Spinning up an [Apache Spark cluster on Amazon EMR](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark.html) may be a quicker way to set all this up.

### Move ETL files over to Spark cluster
Please move over all files in this folder over to your Spark cluster. You can use [SCP with your `my-key-pair.pem` file](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html) in order to do this.

Thereafter, you will need to fill out the empty fields in the `dl.cfg` configuration file with your [AWS access key](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html) that allows you to connect to your S3 buckets.

### Testing
Running the ETL process for this full dataset can take time. Thus, [as suggested by Tran Nguyen](https://towardsdatascience.com/some-issues-when-building-an-aws-data-lake-using-spark-and-how-to-deal-with-these-issues-529ce246ba59), it can be useful to test out the script on a smaller subset of data first.

 If you would like to run the `etl.py` script on a subset of the data, you can do this by running:

`etl.py --run-subset`

This will only load and transform a subset of the data and can help isolate issues in your ETL process without having to process all the data every single time.

## Schema

### Fact Table
`songplays` - a list of times users played a song, extracted from both the `song_data` and `log_data` JSON files: `songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`

### Dimension Tables
`users` - a list of users, extracted from the `log_data` JSON files: `user_id`, `first_name`, `last_name`, `gender`, `level`

`songs` - a list of songs, extracted from the `song_data` JSON files: `song_id`, `title`, `artist_id`, `year`, `duration`

`artists` - a list of artists, extracted from the `song_data` JSON files: `artist_id`, `name`, `location`, `lattitude`, longitude

`time` - a list of timestamps, extracted from the `log_data` JSON files: `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`
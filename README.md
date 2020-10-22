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

# Setup

Please move over all files in this folder over to your Spark cluster. You can use [SCP with your `my-key-pair.pem` file](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html) in order to do this.

Thereafter, you will need to fill out the empty fields in the `dl.cfg` configuration file with your [AWS access key](https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html) that allows you to connect to your S3 buckets.
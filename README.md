# Sparkify-ETL-with-Spark

This project's purpose is to effectively move data from one cloud storage location to another after transforming its structure for the purpose of further analysis. Processing of data is distributed across a cluster of machines in a Hadoop framework.  A Python module is executed across the cluster using the `spark-submit` function.  The entire project uses Amazon Web Services (AWS) products, including S3, EC2 and EMR.

# Installation & Setup

## Files

+ etl.py: Python module containing functions to extract JSON data from S3, transform it using Spark, and load it back into S3 in partitioned Parquet files.
+ dl.cfg: Configuration file with AWS access key and secret access key.
+ copymyfile.sh: Executable bash script that copies etl.py and dl.cfg to a main machine on a cluster of machines (managed in Amazon EMR).

## Amazon S3

All data is stored in S3 (Simple Storage Service) in JSON format.

**input filepaths (us-west-2)**
+ log_data: s3://udacity-dend/log_data
+ song_data: s3://udacity-dend/song_data

**output filepaths (us-east-1)** 
+ s3://udacity-dend-project-output
  + s3://udacity-dend-project-output/song
  + s3://udacity-dend-project-output/artist
  + s3://udacity-dend-project-output/user
  + s3://udacity-dend-project-output/time
  + s3://udacity-dend-project-output/songplay

## PuTTy

PuTTy is a free SSH client for Windows machines. Its use in this project is allowing a connection to the main worker in a cluster where the distributed processing of data takes place.  Installation following the link below and a EC2 key pair for the same main worker/machine are needed to connect via SSH:

http://www.chiark.greenend.org.uk/~sgtatham/putty/download.html

Instructions on obtaining a Key Pair from an EC2 instance on Amazon EC2:

https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#having-ec2-create-your-key-pair

## Amazon EMR Cluster on EC2 Instances

This project uses distributed processing to accelerate the ETL process. A cluster of machines (EC2) on Amazon's Elastic Map Reduce framework completes the work of a single machine in less time. 

For setup on a cluster on EMR, follow these instructions: https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-setting-up.html

I use a cluster of 3 machines, with one main machine and 2 worker machines.

**Bootstrap event**

While launching the cluster of machines on EMR, I choose to load a needed Python package and import the files listed in the "Files" section to the main machine in the cluster. Amazon EMR has an option during cluster configuration called a 'bootstrap' event that accomplishes tasks like this using an executable file. For Linux machines on EC2, I suggest to setup a custom action pointing at the `copymyfile.sh` file. This bash script copies the Python module and configuration file to the main EC2 instance.

# ETL Process

**Process**

The command to run in the main EC2 instance is listed below:

`usr/bin/spark-submit --master yarn etl.py`

This command runs the Python module from the same directory that the configuration file is saved to, and uses the Hadoop YARN scheduler to manage the tasks across the machines in the cluster. The process is then as follows:

Extract from song_data, log_data filepaths -> Load to EMR Cluster -> Transform -> load to output_data filepaths



# Data Table Schema

The data are stored in JSON format, but each JSON file contains tables that follow a predictable format (schema). Below is a reference of the schema for the tables contained in the JSON files under the S3 filepaths for this project:

**s3://udacity-dend/log_data**

Type | Column | Type
-----|--------|------
null | artist | varchar
null | auth | varchar
null | firstname | varchar
null | gender | varchar
null | iteminsession | bigint
null | lastname | varchar
null | length | decimal
null | level | varchar
null | location | varchar
null | method | varchar
null | page | varchar
null | registration | decimal
null | sessionid | int
null | song | varchar
null | status | int
null | ts | bigint
null | useragent | varchar
null | userid | int

**s3://udacity-dend/song_data**

Type | Column | Type
-----|--------|------
null | num_songs | int
null | artist_id | varchar
null | artist_latitude | decimal
null | artist_longitude | decimal
null | artist_name | varchar 
null | song_id | varchar
null | title | varchar
null | duration | decimal
null | year | int


**s3://udacity-dend-project-output/songplay**

Contains data to show the songs listened to by **Sparkify** platform users. Also shows the time and location of when and where the user listened to the song.

Type | Column | Type
-----|--------|------
PK | songplay_id | int
FK | start_time | timestamp
FK | user_id | int
null | level | varchar
FK | song_id | varchar
FK | artist_id | varchar
null | session_id | varchar
null| location | varchar
null | user_agent | varchar

**s3://udacity-dend-project-output/user** 

Contains data on the Sparkify platform user.

Type | Column | Type
-----|--------|------
PK | user_id | int
null | first_name | varchar
null | last_name | varchar
null | gender | varchar
null | level | varchar

Table: **s3://udacity-dend-project-output/song**

Contains song data.

Type | Column | Type
-----|--------|------
PK | song_id | varchar
null | title | varchar
FK | artist_id | varchar
null | year | int
null | duration | decimal
 
**s3://udacity-dend-project-output/artist**

Contains artist data.

Type | Column | Type
-----|--------|------
PK | artist_id | varchar
null | name | varchar
null | location | varchar
null | latitude | varchar
null | longitude | varchar

**s3://udacity-dend-project-output/time**

Contains timestamp data from song listens as well as other time related.

Type | Column | Type
-----|--------|------
PK | start_time | timestamp
null | hour | int
null | day | int
null | week | int
null | month | int
null | year | int
null | weekday | int

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.functions import from_unixtime 

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['spark-admin']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['spark-admin']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + '/song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df[['song_id','title','artist_id','year','duration']]
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year','artist_id']).mode('overwrite').parquet(output_data + '/song/song.parquet')

    # extract columns to create artists table
    artists_table = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    
    # write artists table to parquet files
    artists_table.write.partitionBy(['artist_id','artist_name']).mode('overwrite').parquet(output_data + '/artist/artist.parquet')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + '/log_data/*/*/*.json'
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")
    
    # extract columns for users table    
    users_table = df[['userId','firstName','lastName','gender','level']]

    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + '/users/users_parquet')

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    df = df.withColumn('timestamp', (df['ts']/1000))

    # create datetime column from original timestamp column
    # get_datetime = udf()
    date_convert = udf(lambda x: datetime.fromtimestamp(x))
    df = df.withColumn('datetime', date_convert(df.timestamp))

    # extract columns to create time table
    # start_time, hour, day, week, month, year, weekday
    time_conv = udf(lambda x: x.strftime("%H:%M:%S"))
    hour_conv = udf(lambda x: x.strftime("%H"))
    day_conv = udf(lambda x: x.day)
    week_conv = udf(lambda x: x.strftime("%W"))
    month_conv = udf(lambda x: x.month)
    year_conv = udf(lambda x: x.year)
    weekday_conv = udf(lambda x: x.isoweekday())

    time_table = df.select(time_conv('datetime').alias('start_time'),
                               hour_conv('datetime').alias('hour'),
                               day_conv('datetime').alias('day'),
                               week_conv('datetime').alias('week'),
                               month_conv('datetime').alias('month'),
                               year_conv('datetime').alias('year'),
                               weekday_conv('datetime').alias('weekday'))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').parquet(output_data + '/time/time.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + '/song/song.parquet')
 
    cond = [song_df.title == df.song, song_df.duration == df.length]
    join_df = song_df.join(df, cond, 'inner')
    
    # extract columns from joined song and log datasets to create songplays table 
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    songplays_table = join_df.select( #need a songplay id column
                                     join_df.ts.alias('start_time'),
                                     year_conv('datetime').alias('year'),
                                     month_conv('datetime').alias('month'),
                                     join_df.userId.alias('user_id'),
                                     join_df.level,
                                     join_df.song_id,
                                     join_df.artist_id,
                                     join_df.sessionId.alias('session_id'),
                                     join_df.location,
                                     join_df.userAgent.alias('user_agent'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year','month']).mode('overwrite').parquet(output_data + '/songplay/songplay.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://udacity-dend-project-output"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

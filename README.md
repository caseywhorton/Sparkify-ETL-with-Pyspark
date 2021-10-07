# Sparkify-ETL-with-Pyspark

Files

etl.py
dl.cfg
copyfiles.sh

Setup

Cluster on




ETL Process

Extract from song_data -> Load to EMR Cluster -> Transform in EMR Cluster -> load to output_data



log_data = s3a://udacity-dend/log_data
song_data = s3://udacity-dend/song_data

output_data = s3://udacity-dend-project-output

s3://udacity-dend-project-output/song
s3://udacity-dend-project-output/artist
s3://udacity-dend-project-output/user
s3://udacity-dend-project-output/time
s3://udacity-dend-project-output/songplay

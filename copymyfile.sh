#!/bin/bash
aws s3 cp s3://udacity-dend-project-scripts/etl.py ~/
aws s3 cp s3://udacity-dend-project-scripts/dl.cfg ~/
sudo pip install configparser
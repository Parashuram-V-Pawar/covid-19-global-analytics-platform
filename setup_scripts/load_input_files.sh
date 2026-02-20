#!/usr/bin/env bash

echo "====================================================="
echo "Creating HDFS directory Structure..."
echo "====================================================="
hdfs dfs -mkdir -p /data/
hdfs dfs -mkdir -p /data/covid/
hdfs dfs -mkdir -p /data/covid/raw/
hdfs dfs -mkdir -p /data/covid/staging/
hdfs dfs -mkdir -p /data/covid/curated/
hdfs dfs -mkdir -p /data/covid/analytics/

echo "====================================================="
echo "Loading input files to the HDFS..."
echo "====================================================="
echo "Loading country wise latest data to HDFS..."
hdfs dfs -put -f data/country_wise_latest.csv /data/covid/raw/

echo "Loading covid 19 clean data to HDFS..."
hdfs dfs -put -f data/covid_19_clean_complete.csv /data/covid/raw/

echo "Loading day wise data to HDFS..."
hdfs dfs -put -f data/day_wise.csv /data/covid/raw/

echo "Loading full grouped data to HDFS..."
hdfs dfs -put -f data/full_grouped.csv /data/covid/raw/

echo "Loading usa country wise data to HDFS..."
hdfs dfs -put -f data/usa_county_wise.csv /data/covid/raw/

echo "Loading worldometer data to HDFS..."
hdfs dfs -put -f data/worldometer_data.csv /data/covid/raw/

echo "====================================================="
echo "Verifying files are uploaded..."
echo "====================================================="
hdfs dfs -ls /data/covid/raw/

echo "====================================================="
echo "Analyzing block allocation..."
echo "====================================================="
hdfs fsck /data/covid/raw/country_wise_latest.csv -files -blocks | grep "Total blocks"
hdfs fsck /data/covid/raw/covid_19_clean_complete.csv -files -blocks | grep "Total blocks"
hdfs fsck /data/covid/raw/day_wise.csv -files -blocks | grep "Total blocks"
hdfs fsck /data/covid/raw/full_grouped.csv -files -blocks | grep "Total blocks"
hdfs fsck /data/covid/raw/usa_county_wise.csv -files -blocks | grep "Total blocks"
hdfs fsck /data/covid/raw/worldometer_data.csv -files -blocks | grep "Total blocks"

echo "====================================================="
echo "HDFS file directory and input files setup complete..."
echo "====================================================="
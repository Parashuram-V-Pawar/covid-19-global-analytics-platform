# Task 7: USA Drilldown Analysis
# Using usa_county_wise:
# 1. Aggregate county data to state level.
# 2. Identify top 10 affected states.
# 3. Detect data skew across states.
# 4. Explain skew impact in distributed systems.
# All results must be written to HDFS under /data/covid/analytics.

#---------------------------------------------------------------------------
# Import Statements
#---------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#---------------------------------------------------------------------------
# Starting Session
#---------------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("USA Drilldown Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#---------------------------------------------------------------------------
# Define HDFS Paths
#---------------------------------------------------------------------------
STAGING_PATH = "hdfs:///data/covid/staging/"
ANALYTICS_PATH = "hdfs:///data/covid/analytics/"

#---------------------------------------------------------------------------
# Loading Staging Data
#---------------------------------------------------------------------------
usa_county_wise = spark.read.parquet(STAGING_PATH + "usa_county_wise")

#---------------------------------------------------------------------------
# 1. Aggregate County Data to State Level
#---------------------------------------------------------------------------
state_level_aggregation = usa_county_wise.groupBy("Province_State") \
    .agg(
        avg("Lat").alias("Avg_Lat"),
        avg("Long_").alias("Avg_Long_"),
        count_distinct("Admin2").alias("No_of_Admins"),
        sum("Confirmed").alias("Total Confirmed"),
        sum("Deaths").alias("Total Deaths"),
    )

print("State Level Aggregation")
state_level_aggregation.show()

state_level_aggregation.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "state_level_aggregation")

#---------------------------------------------------------------------------
# 2. Identify Top 10 Affected States
#---------------------------------------------------------------------------
top10_affected_states = state_level_aggregation \
    .select("Province_State", "Total Confirmed") \
    .orderBy(col("Total Confirmed").desc())

print("Top 10 Affected States")
top10_affected_states.limit(10).show()

top10_affected_states.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "top10_affected_states")

#---------------------------------------------------------------------------
# 3. Detect Data Skew Across States
#---------------------------------------------------------------------------
state_distribution = (
    usa_county_wise.groupBy("Province_State")
      .agg(count("*").alias("record_count"))
      .orderBy(col("record_count").desc())
)

stats = state_distribution.agg(
    avg("record_count").alias("mean"),
    stddev("record_count").alias("std")
).first()

mean_value = stats["mean"]
std_value = stats["std"]

state_level_skew = state_distribution.withColumn(
    "skew",
    (col("record_count") - mean_value) / std_value
).orderBy(col("skew").desc())

print("State Level Skew")
state_level_skew.show()

state_level_skew.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "state_level_skew")

#---------------------------------------------------------------------------
# 4. Explain Skew Impact in Distributed Systems
#---------------------------------------------------------------------------
"""
Data skew happens when:
1. Some keys (in this case states) have much more data
2. Other keys have very little

Problems Caused by Skew:
- Uneven CPU usage
- Long shuffle stages
- Memory spill
- Straggler tasks
- Increased job completion time
"""

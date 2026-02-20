#---------------------------------------------------------------------------
# Import Statements
#---------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

#---------------------------------------------------------------------------
# Starting Session
#---------------------------------------------------------------------------
spark = SparkSession.builder.appName("7 Day Rolling Recovery Average").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#---------------------------------------------------------------------------
# Define HDFS Paths
#---------------------------------------------------------------------------
STAGING_PATH = "hdfs:///data/covid/staging/"
ANALYTICS_PATH = "hdfs:///data/covid/analytics/"

#---------------------------------------------------------------------------
# Loading Staging Data
#---------------------------------------------------------------------------
day_wise = spark.read.parquet(STAGING_PATH + "day_wise")

#---------------------------------------------------------------------------
# 2. 7-Day Rolling Recovery Average (Window Function)
#---------------------------------------------------------------------------
recovery_average_rolling_7days = day_wise.select("Date","Recovered")

recovery_average_rolling_7days = recovery_average_rolling_7days.withColumn(
    "7 Day Rolling Recovery Average",
    round(avg("Recovered").over(
        Window.orderBy("Date").rowsBetween(-6, 0)
    ),2)
)

#---------------------------------------------------------------------------
# Writing Output to Analytics Layer
#---------------------------------------------------------------------------
recovery_average_rolling_7days.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "recovery_average_rolling_7days")

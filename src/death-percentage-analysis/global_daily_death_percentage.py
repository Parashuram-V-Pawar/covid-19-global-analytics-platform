# ---------------------------------------------------------
# 1 Import Statements
# ---------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ---------------------------------------------------------
# 2 Start Spark Session
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("Global Death Percentage Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------
# 3 Define HDFS Paths
# ---------------------------------------------------------
STAGING_PATH = "hdfs:///data/covid/staging/"
ANALYTICS_PATH = "hdfs:///data/covid/analytics/"

# ---------------------------------------------------------
# 4 Read Data from Staging Layer
# ---------------------------------------------------------
full_grouped = spark.read.parquet(STAGING_PATH + "full_grouped")

# ---------------------------------------------------------
# 5 Aggregate Global Confirmed & Deaths by Date
# ---------------------------------------------------------
global_deaths = full_grouped.groupBy("Date").agg(
    sum("Confirmed").alias("Global Confirmed"),
    sum("Deaths").alias("Global Deaths")
)

# ---------------------------------------------------------
# 6 Calculate Daily Global Death Percentage
# ---------------------------------------------------------
global_deaths = global_deaths.withColumn(
    "daily death percentage",
    round(
        when(col("Global Confirmed") != 0,
             (col("Global Deaths") / col("Global Confirmed")) * 100
        ).otherwise(0),
        2
    )
)

# ---------------------------------------------------------
# 7 Show Result
# ---------------------------------------------------------
global_deaths.show()

# ---------------------------------------------------------
# 8 Write Output to Analytics Layer
# ---------------------------------------------------------
global_deaths.write \
    .mode("overwrite") \
    .parquet(ANALYTICS_PATH + "global_deaths")

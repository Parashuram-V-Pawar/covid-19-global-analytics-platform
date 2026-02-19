# ---------------------------------------------------------
# 1 Import Statements
# ---------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ---------------------------------------------------------
# 2 Define HDFS Paths (Staging & Analytics Layers)
# ---------------------------------------------------------
STAGING_PATH = "hdfs:///data/covid/staging/"
ANALYTICS_PATH = "hdfs:///data/covid/analytics/"

# ---------------------------------------------------------
# 3 Start Spark Session
# ---------------------------------------------------------
spark = SparkSession.builder.appName("Continent Death Percentage Analysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------
# 4 Read Data from Staging Layer
# ---------------------------------------------------------
full_grouped = spark.read.parquet(STAGING_PATH + "full_grouped")
worldometer_data = spark.read.parquet(STAGING_PATH + "worldometer_data")

# ---------------------------------------------------------
# 5 Join COVID Data with Continent Metadata
# ---------------------------------------------------------
full_worldometer = full_grouped.join(
    worldometer_data,
    "Country/Region",
    "inner"
)

# ---------------------------------------------------------
# 6 Aggregate Confirmed & Deaths by Continent
# ---------------------------------------------------------
continent_wise = full_worldometer.groupBy("Continent").agg(
    sum("Confirmed").alias("Continent Confirmed"),
    sum("Deaths").alias("Continent Deaths")
)

# ---------------------------------------------------------
# 7 Calculate Death Percentage (CFR)
# ---------------------------------------------------------
continent_wise = continent_wise.withColumn(
    "Death Percentage",
    when(col("Continent Confirmed") != 0,
         round((col("Continent Deaths") / col("Continent Confirmed")) * 100, 2)
    ).otherwise(0)
)

# ---------------------------------------------------------
# 8 Show Result
# ---------------------------------------------------------
continent_wise.show()

# ---------------------------------------------------------
# 9 Write Output to Analytics Layer
# ---------------------------------------------------------
continent_wise.write \
    .mode("overwrite") \
    .parquet(ANALYTICS_PATH + "continent_wise")
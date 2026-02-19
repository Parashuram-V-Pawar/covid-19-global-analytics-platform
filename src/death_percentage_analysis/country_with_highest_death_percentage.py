# ---------------------------------------------------------
# 1 Import Statements
# ---------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ---------------------------------------------------------
# 2 Start Spark Session
# ---------------------------------------------------------
spark = SparkSession.builder.appName("Highest Country Death Percentage").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------
# 3 Read Data from Staging Layer
# ---------------------------------------------------------
full_grouped = spark.read.parquet("hdfs:///data/covid/staging/full_grouped")

# ---------------------------------------------------------
# 4 Aggregate Confirmed & Deaths by Country
# ---------------------------------------------------------
country_wise = full_grouped.groupBy("Country/Region").agg(
    sum("Confirmed").alias("Country Confirmed"),
    sum("Deaths").alias("Country Deaths")
)

# ---------------------------------------------------------
# 5 Calculate Death Percentage (CFR)
# ---------------------------------------------------------
country_wise = country_wise.withColumn(
    "Death Percentage",
    when(col("Country Confirmed") != 0,
         round((col("Country Deaths") / col("Country Confirmed")) * 100, 2)
    ).otherwise(0)
)

# ---------------------------------------------------------
# 6 Sort and Get Country with Highest Death Percentage
# ---------------------------------------------------------
country_wise = country_wise.orderBy(
    col("Death Percentage").desc()
).limit(1)

# ---------------------------------------------------------
# 7 Show Result
# ---------------------------------------------------------
country_wise.show()

# ---------------------------------------------------------
# 8 Write Output to Analytics Layer
# ---------------------------------------------------------
country_wise.write \
    .mode("overwrite") \
    .parquet("hdfs:///data/covid/analytics/country_wise")

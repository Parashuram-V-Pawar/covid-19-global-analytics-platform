# ---------------------------------------------------------
# 1 Import Statements
# ---------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ---------------------------------------------------------
# 2 Start Spark Session
# ---------------------------------------------------------
spark = SparkSession.builder.appName("Death Percentage Analysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------
# 3 Read Data from HDFS (Staging Layer)
# ---------------------------------------------------------
full_grouped = spark.read.parquet("hdfs:///data/covid/staging/full_grouped")
worldometer_data = spark.read.parquet("hdfs:///data/covid/staging/worldometer_data")

# ---------------------------------------------------------
# 4 Join COVID Data with Population Data
# ---------------------------------------------------------
full_worldometer = full_grouped.join(
    worldometer_data,
    "Country/Region",
    "inner"
)

# ---------------------------------------------------------
# 5 Aggregate Total Deaths by Country
# ---------------------------------------------------------
country_data = full_worldometer.groupBy("Country/Region") \
    .agg(sum("Deaths").alias("Total Deaths"))

# ---------------------------------------------------------
# 6 Join Population for Per Capita Calculation
# ---------------------------------------------------------
death_per_capita = country_data.join(
    worldometer_data.select("Country/Region", "Population"),
    "Country/Region",
    "inner"
)

# ---------------------------------------------------------
# 7 Calculate Death Per Capita (per 1000)
# ---------------------------------------------------------
death_per_capita = death_per_capita.withColumn(
    "Death per capita(1000)",
    when(col("Population") != 0,
         round(col("Total Deaths") / col("Population") * 1000, 2)
    ).otherwise(0)
)

# ---------------------------------------------------------
# 8 Sort and Get Top 10 Countries
# ---------------------------------------------------------
death_per_capita = death_per_capita.orderBy(
    col("Death per capita(1000)").desc()
).limit(10)

# ---------------------------------------------------------
# 9 Show Result
# ---------------------------------------------------------
death_per_capita.show()

# ---------------------------------------------------------
# 10 Write Output to Analytics Layer
# ---------------------------------------------------------
death_per_capita.write \
    .mode("overwrite") \
    .parquet("hdfs:///data/covid/analytics/death_per_capita")

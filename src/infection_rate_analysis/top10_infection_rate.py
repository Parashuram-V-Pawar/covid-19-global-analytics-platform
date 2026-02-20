# ---------------------------------------------------------
# 1 Import Statements
# ---------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ---------------------------------------------------------
# 2 Start Spark Session
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("Top 10 Infection Rate") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------
# 3 Read worldometer_data
# ---------------------------------------------------------
worldometer_data = spark.read.parquet(
    "hdfs:///data/covid/staging/worldometer_data"
)

# ---------------------------------------------------------
# 4 Calculate Confirmed Per 1000
# ---------------------------------------------------------
cases_per_population = worldometer_data.select(
    "Country/Region", "Population", "TotalCases"
).withColumn(
    "Confirmed Per 1000",
    round((col("TotalCases") / col("Population")) * 1000, 2)
)

# ---------------------------------------------------------
# 5 Get Top 10 Countries
# ---------------------------------------------------------
top10_contries_infection_rate = cases_per_population.orderBy(
    col("Confirmed Per 1000").desc()
).limit(10)

# ---------------------------------------------------------
# 6 Show Output
# ---------------------------------------------------------
top10_contries_infection_rate.show()

# ---------------------------------------------------------
# 7 Write Output to Analytics Layer
# ---------------------------------------------------------
top10_contries_infection_rate.write \
    .mode("overwrite") \
    .parquet("hdfs:///data/covid/analytics/top10_contries_infection_rate")

spark.stop()

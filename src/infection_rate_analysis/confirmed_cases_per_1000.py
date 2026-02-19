# ---------------------------------------------------------
# 1 Import Statements
# ---------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ---------------------------------------------------------
# 2 Start Spark Session
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("Confirmed Cases Per 1000") \
    .master("yarn") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------
# 3 Read worldometer_data
# ---------------------------------------------------------
worldometer_data = spark.read.parquet(
    "hdfs:///data/covid/staging/worldometer_data"
)

# ---------------------------------------------------------
# 4 Calculate Confirmed Cases Per 1000
# ---------------------------------------------------------
cases_per_population = worldometer_data.select(
    "Country/Region", "Population", "TotalCases"
).withColumn(
    "Confirmed Per 1000",
    round((col("TotalCases") / col("Population")) * 1000, 2)
)

# ---------------------------------------------------------
# 5 Show & Write Output
# ---------------------------------------------------------
cases_per_population.show()

# ---------------------------------------------------------
# 6 Write Output to Analytics Layer
# ---------------------------------------------------------
cases_per_population.write \
    .mode("overwrite") \
    .parquet("hdfs:///data/covid/analytics/cases_per_population")

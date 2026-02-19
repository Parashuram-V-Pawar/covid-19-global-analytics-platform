# ---------------------------------------------------------
# 1 Import Statements
# ---------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ---------------------------------------------------------
# 2 Start Spark Session
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("Active Cases Per 1000") \
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
# 4 Calculate Active Cases Per 1000
# ---------------------------------------------------------
active_cases_per_population = worldometer_data.select(
    "Country/Region", "Population", "ActiveCases"
).withColumn(
    "Active Per 1000",
    round((col("ActiveCases") / col("Population")) * 1000, 2)
)

# ---------------------------------------------------------
# 5 Show Output
# ---------------------------------------------------------
active_cases_per_population.show()

# ---------------------------------------------------------
# 6 Write Output to Analytics Layer
# ---------------------------------------------------------
active_cases_per_population.write \
    .mode("overwrite") \
    .parquet("hdfs:///data/covid/analytics/active_cases_per_population")
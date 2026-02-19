# ---------------------------------------------------------
# 1 Import Statements
# ---------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# ---------------------------------------------------------
# 2 Start Spark Session
# ---------------------------------------------------------
spark = SparkSession.builder.appName("Daily Death Percentage").getOrCreate()
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
# 5 Calculate Daily Death Percentage (CFR)
# ---------------------------------------------------------
full_grouped = full_grouped.withColumn(
    "Death Percentage",
    when(col("Confirmed") != 0,
         round((col("Deaths") / col("Confirmed")) * 100, 2)
    ).otherwise(0)
).orderBy(col("Death Percentage"))

# ---------------------------------------------------------
# 6 Select Required Columns
# ---------------------------------------------------------
daily_death_percentage_country = full_grouped \
    .select("Date", "Country/Region", "Death Percentage")

# ---------------------------------------------------------
# 7 Show Result
# ---------------------------------------------------------
daily_death_percentage_country.show()

# ---------------------------------------------------------
# 8 Write Output to Analytics Layer
# ---------------------------------------------------------
daily_death_percentage_country.write \
    .mode("overwrite") \
    .parquet(ANALYTICS_PATH + "daily_death_percentage_country")

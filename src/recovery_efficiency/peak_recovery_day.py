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
spark = SparkSession.builder.appName("Peak Recovery Day").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#---------------------------------------------------------------------------
# Define HDFS Paths
#---------------------------------------------------------------------------
STAGING_PATH = "hdfs:///data/covid/staging/"
ANALYTICS_PATH = "hdfs:///data/covid/analytics/"

#---------------------------------------------------------------------------
# Loading Staging Data
#---------------------------------------------------------------------------
full_grouped = spark.read.parquet(STAGING_PATH + "full_grouped")

#---------------------------------------------------------------------------
# 4. Peak Recovery Day per Country
#---------------------------------------------------------------------------
peak_recovery_day = full_grouped.select(
    "Country/Region","Date","Recovered"
)

peak_recovery_day = peak_recovery_day.withColumn(
    "rank",
    row_number().over(
        Window.partitionBy("Country/Region")
        .orderBy(col("Recovered").desc())
    )
)

peak_recovery_day = peak_recovery_day.select(
    "Country/Region","Date","Recovered"
).filter(col("rank") == 1)

#---------------------------------------------------------------------------
# Writing Output to Analytics Layer
#---------------------------------------------------------------------------
peak_recovery_day.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "peak_recovery_day")
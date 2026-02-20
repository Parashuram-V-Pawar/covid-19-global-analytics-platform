#---------------------------------------------------------------------------
# Import Statements
#---------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#---------------------------------------------------------------------------
# Starting Session
#---------------------------------------------------------------------------
spark = SparkSession.builder.appName("Recovered Percentage Per Country").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#---------------------------------------------------------------------------
# Define HDFS Paths
#---------------------------------------------------------------------------
STAGING_PATH = "hdfs:///data/covid/staging/"
ANALYTICS_PATH = "hdfs:///data/covid/analytics/"

#---------------------------------------------------------------------------
# Loading Staging Data
#---------------------------------------------------------------------------
worldometer_data = spark.read.parquet(STAGING_PATH + "worldometer_data")

#---------------------------------------------------------------------------
# 1. Recovered Percentage per Country
#---------------------------------------------------------------------------
recovered_percentage_country_wise = worldometer_data.select(
    "Country/Region","TotalCases","TotalRecovered"
)

recovered_percentage_country_wise = recovered_percentage_country_wise.withColumn(
    "Recovered Percentage",
    when(col("TotalCases") != 0,
         round((col("TotalRecovered")/col("TotalCases"))*100,2)
    ).otherwise(0)
)

#---------------------------------------------------------------------------
# Writing Output to Analytics Layer
#---------------------------------------------------------------------------
recovered_percentage_country_wise.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "recovered_percentage_country_wise")
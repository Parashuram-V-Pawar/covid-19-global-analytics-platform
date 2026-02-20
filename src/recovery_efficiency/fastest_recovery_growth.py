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
spark = SparkSession.builder.appName("Fastest Recovery Growth").getOrCreate()
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
# 3. Country with Fastest Recovery Growth
#---------------------------------------------------------------------------
fastest_recovery_growth = full_grouped.select(
    "Date","Country/Region","Recovered","New recovered"
)

fastest_recovery_growth = fastest_recovery_growth.withColumn(
    "Previous_Day_Recovery",
    lag("Recovered", 1).over(
        Window.partitionBy("Country/Region").orderBy("Date")
    )
).withColumn(
    "Daily_Recovery_Growth",
    when(col("Previous_Day_Recovery") != 0,
         (col("Recovered") - col("Previous_Day_Recovery"))
         / col("Previous_Day_Recovery") * 100
    ).otherwise(0)
).fillna(0)

fastest_recovery_growth = fastest_recovery_growth.groupBy(
    "Country/Region"
).agg(
    avg(col("Daily_Recovery_Growth")).alias("avg_growth")
).orderBy(col("avg_growth").desc())

fastest_recovery_growth = fastest_recovery_growth.limit(1)

#---------------------------------------------------------------------------
# Writing Output to Analytics Layer
#---------------------------------------------------------------------------
fastest_recovery_growth.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "fastest_recovery_growth")
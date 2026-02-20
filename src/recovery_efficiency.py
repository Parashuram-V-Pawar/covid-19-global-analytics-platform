# Task 5: Recovery Efficiency
# Using worldometer_data, day_wise and full_grouped:
# 1. Recovered percentage per country.
# 2. 7-day rolling recovery average (Window function).
# 3. Country with fastest recovery growth.
# 4. Peak recovery day per country.
# All results must be written to HDFS under /data/covid/analytics.

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
spark = SparkSession.builder.appName("Recovery Efficiency").getOrCreate()
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
day_wise = spark.read.parquet(STAGING_PATH + "day_wise")
full_grouped = spark.read.parquet(STAGING_PATH + "full_grouped")

#---------------------------------------------------------------------------
# 1. Recovered Percentage per Country
#    Formula: TotalRecovered / TotalCases * 100
#---------------------------------------------------------------------------
recovered_percentage_country_wise = worldometer_data.select(
    "Country/Region",
    "TotalCases",
    "TotalRecovered"
)

recovered_percentage_country_wise = recovered_percentage_country_wise.withColumn(
    "Recovered Percentage",
    when(col("TotalCases") != 0,
         round((col("TotalRecovered") / col("TotalCases")) * 100, 2)
    ).otherwise(0)
)

print("Recovered Percentage Per Country")
recovered_percentage_country_wise.show()

recovered_percentage_country_wise.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "recovered_percentage_country_wise")

#---------------------------------------------------------------------------
# 2. 7-Day Rolling Recovery Average (Window Function)
#---------------------------------------------------------------------------
recovery_average_rolling_7days = day_wise.select(
    "Date",
    "Recovered"
)

recovery_average_rolling_7days = recovery_average_rolling_7days.withColumn(
    "7 Day Rolling Recovery Average",
    round(
        avg("Recovered").over(
            Window.orderBy("Date").rowsBetween(-6, 0)
        ),
        2
    )
)

print("7-Day Rolling Recovery Average")
recovery_average_rolling_7days.show()

recovery_average_rolling_7days.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "recovery_average_rolling_7days")

#---------------------------------------------------------------------------
# 3. Country with Fastest Recovery Growth
#---------------------------------------------------------------------------
fastest_recovery_growth = full_grouped.select(
    "Date",
    "Country/Region",
    "Recovered"
)

fastest_recovery_growth = fastest_recovery_growth.withColumn(
    "Previous_Day_Recovery",
    lag("Recovered", 1).over(
        Window.partitionBy("Country/Region").orderBy("Date")
    )
)

fastest_recovery_growth = fastest_recovery_growth.withColumn(
    "Daily_Recovery_Growth",
    when(col("Previous_Day_Recovery") != 0,
         (col("Recovered") - col("Previous_Day_Recovery")) /
         col("Previous_Day_Recovery") * 100
    ).otherwise(0)
).fillna(0)

fastest_recovery_growth = fastest_recovery_growth.groupBy(
    "Country/Region"
).agg(
    avg("Daily_Recovery_Growth").alias("avg_growth")
)

fastest_recovery_growth = fastest_recovery_growth.orderBy(
    col("avg_growth").desc()
).limit(1)

print("Country with Fastest Recovery Growth")
fastest_recovery_growth.show()

fastest_recovery_growth.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "fastest_recovery_growth")

#---------------------------------------------------------------------------
# 4. Peak Recovery Day per Country
#---------------------------------------------------------------------------
peak_recovery_day = full_grouped.select(
    "Country/Region",
    "Date",
    "Recovered"
)

peak_recovery_day = peak_recovery_day.withColumn(
    "rank",
    row_number().over(
        Window.partitionBy("Country/Region")
        .orderBy(col("Recovered").desc())
    )
)

peak_recovery_day = peak_recovery_day.filter(
    col("rank") == 1
).select(
    "Country/Region",
    "Date",
    "Recovered"
)

print("Peak Recovery Day Per Country")
peak_recovery_day.show()

peak_recovery_day.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "peak_recovery_day")
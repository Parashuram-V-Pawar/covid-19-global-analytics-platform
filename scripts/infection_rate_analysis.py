# Task 4: Infection Rate Analysis
# Using worldometer_data:
# 1. Confirmed cases per 1000 population.
# 2. Active cases per 1000 population.
# 3. Top 10 countries by infection rate.
# 4. WHO region infection ranking.
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
spark = SparkSession.builder.appName("Infection Rate Analysis").getOrCreate()
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
full_grouped = spark.read.parquet(STAGING_PATH + "full_grouped")

#---------------------------------------------------------------------------
# 1. Confirmed Cases per 1000 Population
#    Formula: TotalCases / Population * 1000
#---------------------------------------------------------------------------
cases_per_population = worldometer_data.select(
    "Country/Region",
    "Population",
    "TotalCases"
)

cases_per_population = cases_per_population.withColumn(
    "Confirmed Per 1000",
    when(col("Population") != 0,
         round((col("TotalCases") / col("Population")) * 1000, 2)
    ).otherwise(0)
)

print("Confirmed Cases Per 1000 Population")
cases_per_population.show()

cases_per_population.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "cases_per_population")

#---------------------------------------------------------------------------
# 2. Active Cases per 1000 Population
#    Formula: ActiveCases / Population * 1000
#---------------------------------------------------------------------------
active_cases_per_population = worldometer_data.select(
    "Country/Region",
    "Population",
    "ActiveCases"
)

active_cases_per_population = active_cases_per_population.withColumn(
    "Active Per 1000",
    when(col("Population") != 0,
         round((col("ActiveCases") / col("Population")) * 1000, 2)
    ).otherwise(0)
)

print("Active Cases Per 1000 Population")
active_cases_per_population.show()

active_cases_per_population.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "active_cases_per_population")

#---------------------------------------------------------------------------
# 3. Top 10 Countries by Infection Rate
#---------------------------------------------------------------------------
top10_countries_infection_rate = cases_per_population.orderBy(
    col("Confirmed Per 1000").desc()
).limit(10)

print("Top 10 Countries by Infection Rate")
top10_countries_infection_rate.show()

top10_countries_infection_rate.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "top10_countries_infection_rate")

#---------------------------------------------------------------------------
# 4. WHO Region Infection Ranking
#---------------------------------------------------------------------------
full_worldometer = worldometer_data.join(
    full_grouped,
    "Country/Region",
    "inner"
)

who_region_infection_rate = full_worldometer.groupBy(
    "WHO region"
).agg(
    sum("Population").alias("Total Population"),
    sum("TotalCases").alias("Total Cases")
)

who_region_infection_rate = who_region_infection_rate.withColumn(
    "Region Infection Rate Per 1000",
    when(col("Total Population") != 0,
         round((col("Total Cases") / col("Total Population")) * 1000, 2)
    ).otherwise(0)
)

who_region_infection_rate = who_region_infection_rate.orderBy(
    col("Region Infection Rate Per 1000").desc()
)

who_region_infection_rate = who_region_infection_rate.withColumn(
    "Rank",
    dense_rank().over(
        Window.orderBy(col("Region Infection Rate Per 1000").desc())
    )
)

print("WHO Region Infection Ranking")
who_region_infection_rate.show()

who_region_infection_rate.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "who_region_infection_rate")
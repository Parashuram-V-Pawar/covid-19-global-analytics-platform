# Task 3: Death Percentage Analysis
# Using full_grouped.csv:
# 1. Compute daily death percentage per country: Deaths / Confirmed * 100
# 2. Compute global daily death percentage.
# 3. Compute continent-wise death percentage (join with worldometer_data).
# Identify:
# 1. Country with highest death percentage
# 2. Top 10 countries by deaths per capita
# All results must be written to HDFS under /data/covid/analytics.
#---------------------------------------------------------------------------
# Import Statements
#---------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#---------------------------------------------------------------------------
# Starting Session
#---------------------------------------------------------------------------
spark = SparkSession.builder.appName("Death Percentage Analysis").getOrCreate()
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
worldometer_data = spark.read.parquet(STAGING_PATH + "worldometer_data")

#---------------------------------------------------------------------------
# 1. Daily Death Percentage per Country
#    Formula: Deaths / Confirmed * 100
#---------------------------------------------------------------------------
full_grouped = full_grouped.withColumn(
    "Death Percentage",
    when(col("Confirmed") != 0,
         round((col("Deaths") / col("Confirmed")) * 100, 2)
    ).otherwise(0)
).orderBy(col("Death Percentage"))

daily_death_percentage_country = full_grouped.select(
    "Date","Country/Region","Death Percentage"
)

print("Daily Death Percentage Per Country")
daily_death_percentage_country.show()

daily_death_percentage_country.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "daily_death_percentage_country")

#---------------------------------------------------------------------------
# 2. Global Daily Death Percentage
#---------------------------------------------------------------------------
global_deaths = full_grouped.groupBy("Date").agg(
    sum("Confirmed").alias("Global Confirmed"),
    sum("Deaths").alias("Global Deaths")
)

global_deaths = global_deaths.withColumn(
    "daily death percentage",
    round(
        when(col("Global Confirmed") != 0,
             (col("Global Deaths") / col("Global Confirmed")) * 100
        ).otherwise(0),
        2
    )
)

print("Global Daily Death Percentage")
global_deaths.show()

global_deaths.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "global_deaths")

#---------------------------------------------------------------------------
# 3. Continent-wise Death Percentage
#    (Join with worldometer_data)
#---------------------------------------------------------------------------
full_worldometer = full_grouped.join(
    worldometer_data,
    "Country/Region",
    "inner"
)

continent_wise = full_worldometer.groupBy("Continent").agg(
    sum("Confirmed").alias("Continent Confirmed"),
    sum("Deaths").alias("Continent Deaths")
)

continent_wise = continent_wise.withColumn(
    "Death Percentage",
    when(col("Continent Confirmed") != 0,
         round((col("Continent Deaths") / col("Continent Confirmed")) * 100, 2)
    ).otherwise(0)
)

print("Continent-wise Death Percentage")
continent_wise.show()

continent_wise.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "continent_wise")

#---------------------------------------------------------------------------
# 4. Country with Highest Death Percentage
#---------------------------------------------------------------------------
country_wise = full_grouped.groupBy("Country/Region").agg(
    sum("Confirmed").alias("Country Confirmed"),
    sum("Deaths").alias("Country Deaths")
)

country_wise = country_wise.withColumn(
    "Death Percentage",
    when(col("Country Confirmed") != 0,
         round((col("Country Deaths") / col("Country Confirmed")) * 100, 2)
    ).otherwise(0)
)

country_wise = country_wise.orderBy(
    col("Death Percentage").desc()
).limit(1)

print("Country with Highest Death Percentage")
country_wise.show()

country_wise.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "country_wise")

#---------------------------------------------------------------------------
# 5. Top 10 Countries by Deaths per Capita
#---------------------------------------------------------------------------
country_data = full_worldometer.groupBy("Country/Region").agg(
    sum("Deaths").alias("Total Deaths")
)

death_per_capita = country_data.join(
    worldometer_data.select("Country/Region", "Population"),
    "Country/Region",
    "inner"
)

death_per_capita = death_per_capita.withColumn(
    "Death per capita(1000)",
    when(col("Population") != 0,
         round(col("Total Deaths") / col("Population") * 1000, 2)
    ).otherwise(0)
)

death_per_capita = death_per_capita.orderBy(
    col("Death per capita(1000)").desc()
).limit(10)

print("Top 10 Countries by Deaths per Capita")
death_per_capita.show()

death_per_capita.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "death_per_capita")

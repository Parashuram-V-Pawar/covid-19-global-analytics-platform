# Task 8: RDD-Based Implementation
# Using RDD API:
# 1. Calculate total confirmed per country.
# 2. Calculate total deaths per country.
# 3. Compute death percentage using reduceByKey.
# 4. Compare RDD performance vs DataFrame.
# 5. Explain:
#    - Why reduceByKey is preferred over groupByKey
#    - When RDD should be avoided
# All results must be written to HDFS under /data/covid/staging.

#---------------------------------------------------------------------------
# Import Statements
#---------------------------------------------------------------------------
import time
from pyspark.sql import *

#---------------------------------------------------------------------------
# Starting Session
#---------------------------------------------------------------------------
spark = SparkSession.builder\
    .appName("RDD Based implementation")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#---------------------------------------------------------------------------
# Define HDFS Paths
#---------------------------------------------------------------------------
RAW_PATH = "hdfs:///data/covid/raw/"
STAGING_PATH = "/data/covid/staging/"

#---------------------------------------------------------------------------
# Reading Data
#---------------------------------------------------------------------------
rdd_full_grouped = spark.sparkContext.textFile(RAW_PATH + "full_grouped.csv")

header = rdd_full_grouped.first()
rdd_full_grouped = rdd_full_grouped.filter(lambda line: line != header)

#---------------------------------------------------------------------------
# 1. Calculate Total Confirmed per Country
#---------------------------------------------------------------------------
rdd_country_confirmed = rdd_full_grouped.map(lambda line: line.split(",")) \
    .map(lambda columns: (columns[1], int(columns[2]))) \
    .reduceByKey(lambda confirmed1, confirmed2: confirmed1 + confirmed2)

result = rdd_country_confirmed.collect()
print("\n\nCountry wise confirmed:")
print("----------------------------")
print("Country -> Confirmed cases")
print("----------------------------")
for country, confirmed in result:
    print(f"{country} -> {confirmed}")

df_country_confirmed = rdd_country_confirmed.map(
    lambda row: Row(Country=row[0], Deaths=row[1])
).toDF()

df_country_confirmed.write \
    .mode("overwrite") \
    .option("header", True) \
    .parquet(STAGING_PATH + "rdd_countrywise_confirmed")

#---------------------------------------------------------------------------
# 2. Calculate Total Deaths per Country
#---------------------------------------------------------------------------
rdd_country_deaths = rdd_full_grouped.map(lambda line: line.split(",")) \
    .map(lambda columns: (columns[1], int(columns[3]))) \
    .reduceByKey(lambda deaths1, deaths2: deaths1 + deaths2)

result = rdd_country_deaths.collect()
print("\n\nCountry wise Deaths:")
print("----------------------------")
print("Country -> Deaths")
print("----------------------------")
for country, deaths in result:
    print(f"{country} -> {deaths}")

df_country_deaths = rdd_country_deaths.map(
    lambda row: Row(Country=row[0], Deaths=row[1])
).toDF()

df_country_deaths.write \
    .mode("overwrite") \
    .option("header", True) \
    .parquet(STAGING_PATH + "rdd_countrywise_deaths")

#---------------------------------------------------------------------------
# 3. Compute Death Percentage Using reduceByKey
#---------------------------------------------------------------------------
confirmed_and_deaths_joined = rdd_country_confirmed.join(rdd_country_deaths)

rdd_death_percentage_per_country = confirmed_and_deaths_joined.map(
    lambda country_data: (
        country_data[0],
        round(
            (country_data[1][1] / country_data[1][0]) * 100,
            2
        ) if country_data[1][0] > 0 else 0
    )
)

result = rdd_death_percentage_per_country.collect()
print("\n\nCountry wise death percentage:")
print("----------------------------")
print("Country -> Death Percentage")
print("----------------------------")
for country, death_percentage in result:
    print(f"{country} -> {death_percentage}")

df_death_percentage_per_country = rdd_death_percentage_per_country.map(
    lambda row: Row(Country=row[0], Death_Percentage=row[1])
).toDF()

df_death_percentage_per_country.write \
    .mode("overwrite") \
    .option("header", True) \
    .parquet(STAGING_PATH + "rdd_death_percentage_countrywise")

#---------------------------------------------------------------------------
# 4. Compare RDD Performance vs DataFrame
#---------------------------------------------------------------------------
print("\n\nPerformance comparision: ")
start_time = time.time()
rdd_full_grouped = spark.sparkContext.textFile(RAW_PATH + "full_grouped.csv")
header = rdd_full_grouped.first()
rdd_full_grouped = rdd_full_grouped.filter(lambda line: line != header)
rdd_country_confirmed = rdd_full_grouped.map(lambda line: line.split(",")) \
    .map(lambda columns: (columns[1], int(columns[2]))) \
    .reduceByKey(lambda confirmed1, confirmed2: confirmed1 + confirmed2)
rdd_country_confirmed.count()
rdd_time = time.time() - start_time
print(f"RDD Execution Time: {rdd_time:.2f} seconds")

start_time = time.time()
df_full_grouped = spark.read.csv(RAW_PATH + "full_grouped.csv", header=True, inferSchema=True)
df_country_confirmed = df_full_grouped.groupBy("Country/Region").sum("Confirmed")
df_country_confirmed.count()
df_time = time.time() - start_time
print(f"DataFrame Execution Time: {df_time:.2f} seconds")

if rdd_time > df_time:
    print("DataFrame is faster")
else:
    print("RDD is faster")

#---------------------------------------------------------------------------
# 5. Why reduceByKey is Preferred Over groupByKey
#---------------------------------------------------------------------------
"""
groupByKey():
- Sends all values of a key across the network
- Then performs aggregation
- No pre-aggregation on mapper side

reduceByKey():
- Performs local aggregation first (map-side combine)
- Then shuffles only reduced results
- Much less network traffic
"""

#---------------------------------------------------------------------------
# 6. When RDD Should Be Avoided
#---------------------------------------------------------------------------
"""
1. Structured Data Exists
2. You Need Performance Optimization
3. Large Aggregations / Joins
4. You Need SQL Capability
5. Production Systems
"""
# Task 9: Spark SQL Implementation
# Create temporary views.
# Write SQL queries for:
# 1. Top 10 infection countries
# 2. Death percentage ranking
# 3. Rolling 7-day average
# Compare physical plans with DataFrame API.
# All results must be written to HDFS under /data/covid/analytics.

#---------------------------------------------------------------------------
# Import Statements
#---------------------------------------------------------------------------
from pyspark.sql import SparkSession

#---------------------------------------------------------------------------
# Starting Spark Session
#---------------------------------------------------------------------------
spark = SparkSession.builder\
    .appName("Spark SQL implementation")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#---------------------------------------------------------------------------
# Define HDFS Paths
#---------------------------------------------------------------------------
STAGING_PATH = "hdfs:///data/covid/staging/"
ANALYTICS_PATH = "hdfs:///data/covid/analytics/"

#---------------------------------------------------------------------------
# Reading Data
#---------------------------------------------------------------------------
worldometer_data = spark.read.parquet(STAGING_PATH + "worldometer_data")
day_wise = spark.read.parquet(STAGING_PATH + "day_wise")

#---------------------------------------------------------------------------
# Create Temporary Views
#---------------------------------------------------------------------------
worldometer_data.createOrReplaceTempView("worldometer_table")
day_wise.createOrReplaceTempView("day_wise_table")

#---------------------------------------------------------------------------
# 1. Top 10 Infection Countries
#---------------------------------------------------------------------------
sql_top10_infected_countries = spark.sql("""
SELECT `Country/Region` AS Country,
       `TotalCases`,
       `Population`,
       ROUND(TotalCases/Population * 1000,2) AS Confirmed_Per_1000
FROM worldometer_table
ORDER BY Confirmed_Per_1000 DESC
LIMIT 10
""")

print("\n\nTop 10 Infected Countries:")
sql_top10_infected_countries.show()
sql_top10_infected_countries.write \
    .mode("overwrite") \
    .parquet(ANALYTICS_PATH + "sql_top10_infected_countries")

#---------------------------------------------------------------------------
# 2. Death Percentage Ranking
#---------------------------------------------------------------------------
sql_death_percentage_ranking = spark.sql("""
WITH death_percentage_calc AS (
    SELECT `Country/Region` AS Country,
           TotalDeaths/TotalCases * 100 AS Death_Percentage
    FROM worldometer_table
)
SELECT Country,
       Death_Percentage,
       DENSE_RANK() OVER(ORDER BY Death_Percentage DESC) AS Rank
FROM death_percentage_calc
""")

print("\n\nDeath Percentage Ranking:")
sql_death_percentage_ranking.show()
sql_death_percentage_ranking.write \
    .mode("overwrite") \
    .parquet(ANALYTICS_PATH + "sql_death_percentage_ranking")

#---------------------------------------------------------------------------
# 3. Rolling 7-Day Average
#---------------------------------------------------------------------------
sql_7day_rolling_average = spark.sql("""
WITH select_columns AS (
    SELECT Date, Confirmed, Deaths, Recovered
    FROM day_wise_table
)
SELECT Date,
       Confirmed,
       ROUND(AVG(Confirmed) OVER(
            ORDER BY Date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ),2) AS confirmed_rolling_avg,
       Deaths,
       ROUND(AVG(Deaths) OVER(
            ORDER BY Date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ),2) AS deaths_rolling_avg,
       Recovered,
       ROUND(AVG(Recovered) OVER(
            ORDER BY Date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
       ),2) AS recovered_rolling_avg
FROM select_columns
""")

print("\n\n7 Days Rolling Average:")
sql_7day_rolling_average.show()
sql_7day_rolling_average.write \
    .mode("overwrite") \
    .parquet(ANALYTICS_PATH + "sql_7day_rolling_average")

#---------------------------------------------------------------------------
# 4. Compare Physical Plans with DataFrame API
#---------------------------------------------------------------------------
# Top 10 Infection Countries – DataFrame API
df_top10_infected = worldometer_data \
    .selectExpr("`Country/Region` AS Country",
                "TotalCases",
                "Population",
                "ROUND(TotalCases/Population * 1000,2) AS Confirmed_Per_1000") \
    .orderBy("Confirmed_Per_1000", ascending=False) \
    .limit(10)

print("SQL Physical Plan:")
sql_top10_infected_countries.explain()

print("\nDataFrame Physical Plan:")
df_top10_infected.explain()
print("-----------------------------------------------------------------------------")

# Death Percentage Ranking – DataFrame API
window_spec = Window.orderBy(col("Death_Percentage").desc())

df_death_percentage = worldometer_data \
    .selectExpr("`Country/Region` AS Country",
                "TotalDeaths",
                "TotalCases",
                "(TotalDeaths/TotalCases) * 100 AS Death_Percentage") \
    .withColumn("Rank", dense_rank().over(window_spec))

print("\nSQL Physical Plan:")
sql_death_percentage_ranking.explain()

print("\nDataFrame Physical Plan:")
df_death_percentage.explain()
print("-----------------------------------------------------------------------------")

# Rolling 7-Day Average – DataFrame API
window_7day = Window.orderBy("Date").rowsBetween(-6, 0)

df_rolling_avg = day_wise \
    .select("Date", "Confirmed", "Deaths", "Recovered") \
    .withColumn("confirmed_rolling_avg",
                round(avg("Confirmed").over(window_7day), 2)) \
    .withColumn("deaths_rolling_avg",
                round(avg("Deaths").over(window_7day), 2)) \
    .withColumn("recovered_rolling_avg",
                round(avg("Recovered").over(window_7day), 2))

print("\nSQL Physical Plan:")
sql_7day_rolling_average.explain()

print("\nDataFrame Physical Plan:")
df_rolling_avg.explain()
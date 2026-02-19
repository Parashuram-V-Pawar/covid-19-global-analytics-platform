#---------------------------------------------------------------------------
# Import Statements
#---------------------------------------------------------------------------
import subprocess
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *

#---------------------------------------------------------------------------
# Starting Session
#---------------------------------------------------------------------------
spark = SparkSession.builder.appName("Data Ingestion").getOrCreate()

RAW_PATH = "hdfs:///data/covid/raw/"
STAGING_PATH = "hdfs:///data/covid/staging/"

#---------------------------------------------------------------------------
# schema for covid_19_clean_complete table
#---------------------------------------------------------------------------
covid_clean_schema = StructType([
    StructField("Province/State", StringType(), True),
    StructField("Country/Region", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long", DoubleType(), True),
    StructField("Date", DateType(), True),
    StructField("Confirmed", LongType(), True),
    StructField("Deaths", LongType(), True),
    StructField("Recovered", LongType(), True),
    StructField("Active", LongType(), True),
    StructField("WHO Region", StringType(), True)
])
# Loading data using defined schema.
covid_clean = spark.read.option("header",True).schema(covid_clean_schema).csv(RAW_PATH + "covid_19_clean_complete.csv")
# Handling null values.
covid_clean = covid_clean.na.drop(subset=["Date", "Country/Region"])
covid_clean = covid_clean.na.fill({
    "Province/State": "Unknown",
    "Lat": 0.0,
    "Long": 0.0,
    "Confirmed": 0,
    "Deaths": 0,
    "Recovered": 0,
    "Active": 0,
    "WHO Region": "Unknown"
})
# Converting raw data into paraquet format.
covid_clean.write.mode("overwrite").parquet(STAGING_PATH + "covid_clean")

#---------------------------------------------------------------------------
# Schema for full_grouped table
#---------------------------------------------------------------------------
full_grouped_schema = StructType([
    StructField("Date", DateType(), True),
    StructField("Country/Region", StringType(), True),
    StructField("Confirmed", LongType(), True),
    StructField("Deaths", LongType(), True),
    StructField("Recovered", LongType(), True),
    StructField("Active", LongType(), True),
    StructField("New cases", LongType(), True),
    StructField("New deaths", LongType(), True),
    StructField("New recovered", LongType(), True),
    StructField("WHO Region", StringType(), True)
])
# Loading data using defined schema.
full_grouped = spark.read.option("header",True).schema(full_grouped_schema).csv(RAW_PATH + "full_grouped.csv")
# Handling null values.
full_grouped = full_grouped.na.drop(subset=["Date", "Country/Region"])
full_grouped = full_grouped.na.fill({
    "Confirmed": 0,
    "Deaths": 0,
    "Recovered": 0,
    "Active": 0,
    "New cases": 0,
    "New deaths": 0,
    "New recovered": 0,
    "WHO Region": "Unknown"
})
# Converting raw data into paraquet format.
full_grouped.write.mode("overwrite").parquet(STAGING_PATH + "full_grouped")

#---------------------------------------------------------------------------
# Schema for country_wise_latest table
#---------------------------------------------------------------------------
country_wise_latest_schema = StructType([
    StructField("Country/Region", StringType(), True),
    StructField("Confirmed", LongType(), True),
    StructField("Deaths", LongType(), True),
    StructField("Recovered", LongType(), True),
    StructField("Active", LongType(), True),
    StructField("New cases", LongType(), True),
    StructField("New deaths", LongType(), True),
    StructField("New recovered", LongType(), True),
    StructField("Deaths / 100 Cases", DoubleType(), True),
    StructField("Recovered / 100 Cases", DoubleType(), True)
])
# Loading data using defined schema.
country_wise_latest = spark.read.option("header",True).schema(country_wise_latest_schema).csv(RAW_PATH + "country_wise_latest.csv")
# Handling null values.
country_wise_latest = country_wise_latest.na.drop(subset=["Country/Region"])
country_wise_latest = country_wise_latest.na.fill({
    "Confirmed": 0,
    "Deaths": 0,
    "Recovered": 0,
    "Active": 0,
    "New cases": 0,
    "New deaths": 0,
    "New recovered": 0,
    "Deaths / 100 Cases": 0.0,
    "Recovered / 100 Cases": 0.0
})
# Converting raw data into paraquet format.
country_wise_latest.write.mode("overwrite").parquet(STAGING_PATH + "country_wise_latest")

#---------------------------------------------------------------------------
# Schema for day_wise table
#---------------------------------------------------------------------------
day_wise_schema = StructType([
    StructField("Date", DateType(), True),
    StructField("Confirmed", LongType(), True),
    StructField("Deaths", LongType(), True),
    StructField("Recovered", LongType(), True),
    StructField("Active", LongType(), True),
    StructField("New cases", LongType(), True),
    StructField("New deaths", LongType(), True),
    StructField("New recovered", LongType(), True),
    StructField("Deaths / 100 Cases", DoubleType(), True),
    StructField("Recovered / 100 Cases", DoubleType(), True)
])
# Loading data using defined schema.
day_wise = spark.read.option("header",True).schema(day_wise_schema).csv(RAW_PATH + "day_wise.csv")
# Handling null values.
day_wise = day_wise.na.drop(subset=["Date"])
day_wise = day_wise.na.fill({
    "Confirmed": 0,
    "Deaths": 0,
    "Recovered": 0,
    "Active": 0,
    "New cases": 0,
    "New deaths": 0,
    "New recovered": 0,
    "Deaths / 100 Cases": 0.0,
    "Recovered / 100 Cases": 0.0
})
# Converting raw data into paraquet format.
day_wise.write.mode("overwrite").parquet(STAGING_PATH + "day_wise")

#---------------------------------------------------------------------------
# Schema for usa_country_wise table
#---------------------------------------------------------------------------
usa_county_wise_schema = StructType([
    StructField("UID", LongType(), True),
    StructField("iso2", StringType(), True),
    StructField("iso3", StringType(), True),
    StructField("code3", IntegerType(), True),
    StructField("FIPS", DoubleType(), True),
    StructField("Admin2", StringType(), True),
    StructField("Province_State", StringType(), True),
    StructField("Country_Region", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Long_", DoubleType(), True)
])
# Loading data using defined schema.
usa_country_wise = spark.read.option("header",True).schema(usa_county_wise_schema).csv(RAW_PATH + "usa_county_wise.csv")
# Handling null values.
usa_country_wise = usa_country_wise.na.drop(subset=["UID", "Province_State"])
usa_country_wise = usa_country_wise.na.fill({
    "iso2": "Unknown",
    "iso3": "Unknown",
    "Admin2": "Unknown",
    "Country_Region": "Unknown",
    "Lat": 0.0,
    "Long_": 0.0
})
# Converting raw data into paraquet format.
usa_country_wise.write.mode("overwrite").parquet(STAGING_PATH + "usa_country_wise")

#---------------------------------------------------------------------------
# Schema for worldometer_data table
#---------------------------------------------------------------------------
worldometer_data_schema = StructType([
    StructField("Country/Region", StringType(), True),
    StructField("Continent", StringType(), True),
    StructField("Population", LongType(), True),
    StructField("TotalCases", LongType(), True),
    StructField("NewCases", LongType(), True),
    StructField("TotalDeaths", LongType(), True),
    StructField("NewDeaths", LongType(), True),
    StructField("TotalRecovered", LongType(), True),
    StructField("NewRecovered", LongType(), True),
    StructField("ActiveCases", LongType(), True)
])
# Loading data using defined schema.
worldometer_data = spark.read.option("header",True).schema(worldometer_data_schema).csv(RAW_PATH + "worldometer_data.csv")
# Handling null values.
worldometer_data = worldometer_data.na.drop(subset=["Country/Region", "Population"])
worldometer_data = worldometer_data.na.fill({
    "Continent": "Unknown",
    "TotalCases": 0,
    "NewCases": 0,
    "TotalDeaths": 0,
    "NewDeaths": 0,
    "TotalRecovered": 0,
    "NewRecovered": 0,
    "ActiveCases": 0
})
# Converting raw data into paraquet format.
worldometer_data.write.mode("overwrite").parquet(STAGING_PATH + "worldometer_data")

# -------------------------------------------------------------------------
# Compare File Size in Hadoop and Parquet
# -------------------------------------------------------------------------
print("File size in Hadoop...")
subprocess.run(["hdfs", "dfs", "-du", "-h", "/data/covid/raw"])
print("\nFile size in parquet...")
subprocess.run(["hdfs", "dfs", "-du", "-h", "/data/covid/staging"])

# -------------------------------------------------------------------------
# Measure read performance
# -------------------------------------------------------------------------
print('''
Measure read performance
---------------------------------------------------------------------------''')
start_time = time.time()
df_country_wise_latest = spark.read \
    .option('header', True) \
    .schema(country_wise_latest_schema) \
    .csv(RAW_PATH + 'country_wise_latest.csv') \
    .count()

df_covid_19_clean_complete = spark.read \
    .option('header', True) \
    .schema(covid_clean_schema) \
    .csv(RAW_PATH + 'covid_19_clean_complete.csv') \
    .count()

df_day_wise = spark.read \
    .option('header', True) \
    .schema(day_wise_schema) \
    .csv(RAW_PATH + 'day_wise.csv') \
    .count()

df_full_grouped = spark.read \
    .option('header', True) \
    .schema(full_grouped_schema) \
    .csv(RAW_PATH + 'full_grouped.csv') \
    .count()

df_usa_country_wise = spark.read \
    .option('header', True) \
    .schema(usa_county_wise_schema) \
    .csv(RAW_PATH + 'usa_county_wise.csv') \
    .count()

df_worldometer_data = spark.read \
    .option('header', True) \
    .schema(worldometer_data_schema) \
    .csv(RAW_PATH + 'worldometer_data.csv') \
    .count()
    
end_time = time.time()
print(f'''
---------------------------------------------------------------------------
CSV read time {end_time - start_time} seconds
''')

start_time = time.time()
df_country_wise_latest = spark.read.parquet(STAGING_PATH + "country_wise_latest").count()
df_covid_19_clean_complete = spark.read.parquet(STAGING_PATH + "covid_clean").count()
df_day_wise = spark.read.parquet(STAGING_PATH + "day_wise").count()
df_full_grouped = spark.read.parquet(STAGING_PATH + "full_grouped").count()
df_usa_county_wise = spark.read.parquet(STAGING_PATH + "usa_country_wise").count()
df_worldometer_data = spark.read.parquet(STAGING_PATH + "worldometer_data").count()
end_time = time.time()
print(f'''
---------------------------------------------------------------------------
Parquet read time {end_time - start_time} seconds
''')

#---------------------------------------------------------------------------
# Listing all staged parquet files.
#---------------------------------------------------------------------------
print("Listing stored 'Parquet' files...........................")
subprocess.run(["hdfs", "dfs", "-ls", "/data/covid/staging/"])
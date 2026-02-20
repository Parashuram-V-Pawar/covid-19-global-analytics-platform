# Task 6: Global Time-Series Analysis
# Using day_wise.csv:
# 1. Global daily average new cases.
# 2. Detect spike days using Z-score.
# 3. Identify peak death date globally.
# 4. Month-over-Month death growth rate.
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
spark = SparkSession.builder.appName("Global Time Series Analysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#---------------------------------------------------------------------------
# Define HDFS Paths
#---------------------------------------------------------------------------
STAGING_PATH = "hdfs:///data/covid/staging/"
ANALYTICS_PATH = "hdfs:///data/covid/analytics/"

#---------------------------------------------------------------------------
# Loading Staging Data
#---------------------------------------------------------------------------
day_wise = spark.read.parquet(STAGING_PATH + "day_wise")

#---------------------------------------------------------------------------
# 1. Global Daily Average New Cases
#---------------------------------------------------------------------------
daily_average_new_cases = day_wise.select("Date","New cases")

daily_average_new_cases = daily_average_new_cases.withColumn(
    "Avg daily new cases",
    avg("New cases").over(
        Window.orderBy("Date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
)

print("Global Daily Average New Cases")
daily_average_new_cases.show()

daily_average_new_cases.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "daily_average_new_cases")

#---------------------------------------------------------------------------
# 2. Detect Spike Days Using Z-Score
#---------------------------------------------------------------------------
window_spec = Window.rowsBetween(
    Window.unboundedPreceding,
    Window.unboundedFollowing
)

zscore_spike_days = day_wise.select(
    "Date","New cases"
).withColumn(
    "mean",
    round(avg("New cases").over(window_spec), 2)
).withColumn(
    "stddev",
    round(stddev("New cases").over(window_spec),2)
).withColumn(
    "Z_score",
    round((col("New cases") - col("mean")) / col("stddev"), 2)
)

zscore_spike_days = zscore_spike_days.filter(col("Z_score") > 2)

print("Spike Days Based on Z-Score (> 2)")
zscore_spike_days.show()

zscore_spike_days.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "zscore_spike_days")

#---------------------------------------------------------------------------
# 3. Identify Peak Death Date Globally
#---------------------------------------------------------------------------
peak_death_day_globally = day_wise.select(
    "Date","Deaths"
).orderBy(col("Deaths").desc()).limit(1)

print("Peak Death Date Globally")
peak_death_day_globally.show()

peak_death_day_globally.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "peak_death_day_globally")

#---------------------------------------------------------------------------
# 4. Month-over-Month Death Growth Rate
#---------------------------------------------------------------------------
monthly_death_growth_rate = day_wise.select(
    "Date","Deaths","New deaths"
)

monthly_death_growth_rate = monthly_death_growth_rate.withColumn(
    "Month", month(col("Date"))
).withColumn(
    "Year", year(col("Date"))
)

monthly_death_growth_rate = monthly_death_growth_rate.groupBy(
    "Year","Month"
).agg(
    sum("New deaths").alias("Monthly Deaths")
).orderBy("Year","Month")

monthly_death_growth_rate = monthly_death_growth_rate.withColumn(
    "Previous_Month_Deaths",
    lag("Monthly Deaths").over(
        Window.orderBy("Year","Month")
    )
).withColumn(
    "Monthly death growth rate",
    round(
        (col("Monthly Deaths") - col("Previous_Month_Deaths"))
        / col("Previous_Month_Deaths") * 100,2
    )
)

print("Month-over-Month Death Growth Rate")
monthly_death_growth_rate.show()

monthly_death_growth_rate.write.mode("overwrite") \
    .parquet(ANALYTICS_PATH + "monthly_death_growth_rate")
